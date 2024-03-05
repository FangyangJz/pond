# !/usr/bin/env python3
# @Datetime : 2023/11/7 23:00
# @Author   : Fangyang
# @Software : PyCharm
import gc
import time
import pandas as pd
import polars as pl
from pathlib import Path
from loguru import logger
from tqdm import tqdm
from datetime import time as dtime
import pond.akshare.stock.industry as pindustry

from duckdb import DuckDBPyRelation
from pond.duckdb import DuckDB
from pond.duckdb.stock.level2 import (
    get_level2_daily_df_with_threading,
    get_trade_script,
    get_order_script,
    get_orderbook_script,
    Task,
)
from pond.duckdb.type import DataFrameStrType, df_types, DataFrameType


class StockDB(DuckDB):
    def __init__(self, db_path: Path, df_type: DataFrameStrType = df_types.polars):
        self.path_stock = db_path / "stock"
        self.path_stock_info = self.path_stock / "info"
        self.path_stock_kline_1m = self.path_stock / "kline_1m"
        self.path_stock_snapshot_1m = self.path_stock / "snapshot_1m"
        self.path_stock_kline_1d = self.path_stock / "kline_1d"
        self.path_stock_kline_1d_nfq = self.path_stock_kline_1d / "nfq"
        self.path_stock_kline_1d_qfq = self.path_stock_kline_1d / "qfq"
        self.path_stock_level2 = self.path_stock / "level2"
        self.path_stock_level2_origin = self.path_stock_level2 / "origin"
        self.path_stock_level2_trade = self.path_stock_level2 / "trade"
        self.path_stock_level2_trade_agg = self.path_stock_level2 / "trade_agg"
        self.path_stock_level2_order = self.path_stock_level2 / "order"
        self.path_stock_level2_orderbook = self.path_stock_level2 / "orderbook"
        self.path_stock_level2_orderbook_rebuild = (
            self.path_stock_level2 / "orderbook_rebuild"
        )
        self.path_stock_list = [
            self.path_stock,
            self.path_stock_info,
            self.path_stock_kline_1d,
            self.path_stock_kline_1m,
            self.path_stock_kline_1d_nfq,
            self.path_stock_kline_1d_qfq,
            self.path_stock_level2,
            self.path_stock_level2_origin,
            self.path_stock_level2_trade,
            self.path_stock_level2_trade_agg,
            self.path_stock_level2_order,
            self.path_stock_level2_orderbook,
            self.path_stock_level2_orderbook_rebuild,
            self.path_stock_snapshot_1m,
            self.path_stock_industry
        ]

        super().__init__(db_path, df_type)

    def init_db_path(self):
        [f.mkdir() for f in self.path_stock_list if not f.exists()]

    @property
    def stock_basic_df(self) -> DataFrameType:
        return self.transform_to_df(
            self.con.sql(
                rf"SELECT * from read_parquet('{str(self.path_stock_info / 'basic.parquet')}')"
            )
        )

    @property
    def calender_df(self) -> DataFrameType:
        return self.transform_to_df(
            self.con.sql(
                rf"SELECT * from read_parquet('{str(self.path_stock_info / 'calender.parquet')}')"
            )
        )

    def update_stock_info(self):
        from pond.akshare.stock import get_all_stocks_df
        from akshare import tool_trade_date_hist_sina

        start_time = time.perf_counter()

        stock_basic_df = get_all_stocks_df()

        calender_df = tool_trade_date_hist_sina().astype("str")
        calender_df["trade_date"] = pd.to_datetime(calender_df["trade_date"])
        calender_df.reset_index(inplace=True)

        (
            self.con.sql("select * from stock_basic_df").write_parquet(
                str(self.path_stock_info / "basic.parquet"), compression=self.compress
            )
        )
        logger.success(
            f"Update {stock_basic_df.shape} basic.parquet cost: {time.perf_counter() - start_time}s"
        )

        (
            self.con.sql("select * from calender_df").write_parquet(
                str(self.path_stock_info / "calender.parquet"),
                compression=self.compress,
            )
        )
        logger.success(
            f"Update calender.parquet cost: {time.perf_counter() - start_time}s"
        )

    def update_level2_trade(self):
        """
        No use duckdb read csv file directly because duckdb doesn't support GBK encoding.
        Need to read by pandas and then map to duckdb.
        """

        for dir_path in self.path_stock_level2_origin.glob("*"):
            if dir_path.is_dir():
                date_str = dir_path.stem
                target_file = self.path_stock_level2_trade / f"{date_str}.parquet"
                if target_file.exists():
                    continue
                df = get_level2_daily_df_with_threading(Task(dir_path).trade)
                start_time = time.perf_counter()
                (
                    self.con.sql(get_trade_script()).write_parquet(
                        str(target_file),
                        compression=self.compress,
                    )
                )
                logger.success(
                    f"Update {df.shape} level2 trade {date_str}.parquet, "
                    f"time cost: {time.perf_counter() - start_time:.4f}s"
                )
                gc.collect()

    def update_level2_order(self):
        """
        No use duckdb read csv file directly because duckdb doesn't support GBK encoding.
        Need to read by pandas and then map to duckdb.
        """

        for dir_path in self.path_stock_level2_origin.glob("*"):
            if dir_path.is_dir():
                date_str = dir_path.stem
                target_file = self.path_stock_level2_order / f"{date_str}.parquet"
                if target_file.exists():
                    continue

                df = get_level2_daily_df_with_threading(Task(dir_path).order)

                start_time = time.perf_counter()
                (
                    self.con.sql(get_order_script()).write_parquet(
                        str(target_file),
                        compression=self.compress,
                    )
                )
                logger.success(
                    f"Update {df.shape} level2 order {date_str}.parquet, "
                    f"time cost: {time.perf_counter() - start_time:.4f}s"
                )
                gc.collect()

    def update_level2_orderbook(self):
        """
        No use duckdb read csv file directly because duckdb doesn't support GBK encoding.
        Need to read by pandas and then map to duckdb.
        """

        for dir_path in self.path_stock_level2_origin.glob("*"):
            if dir_path.is_dir():
                date_str = dir_path.stem
                target_file = self.path_stock_level2_orderbook / f"{date_str}.parquet"
                if target_file.exists():
                    continue

                df = get_level2_daily_df_with_threading(Task(dir_path).orderbook)

                start_time = time.perf_counter()
                (
                    self.con.sql(get_orderbook_script()).write_parquet(
                        str(target_file),
                        compression=self.compress,
                    )
                )
                logger.success(
                    f"Update {df.shape} level2 orderbook {date_str}.parquet, "
                    f"time cost: {time.perf_counter() - start_time:.4f}s"
                )
                gc.collect()

    def update_kline_1d_nfq(self, offset: int = 0):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
        :return:
        """
        from pond.duckdb.stock.kline import get_kline_1d_nfq_df

        start_time = time.perf_counter()

        logger.info("Start to read local tdx stock 1d data ...")
        df = get_kline_1d_nfq_df(stock_basic_df=self.stock_basic_df, offset=offset)

        files = [f.stem for f in self.path_stock_kline_1d_nfq.iterdir()]
        logger.info("Start to write parquet file by date ...")
        for idx, group_df in (
            pbar2 := tqdm(df.groupby("trade_date"), position=0, leave=True)
        ):
            file_name = str(idx.date()).replace("-", "")
            pbar2.set_postfix_str(file_name)

            if file_name not in files:
                (
                    self.con.sql("select * from group_df").write_parquet(
                        str(self.path_stock_kline_1d_nfq / f"{file_name}.parquet"),
                        compression=self.compress,
                    )
                )

        pbar2.close()
        logger.success(
            f"Update all parquet file cost: {time.perf_counter() - start_time:.2f}s"
        )

    def get_kline_1d_qfq_rel(
        self, start_date: str = "2023-01-01", end_date: str = "2070-01-01"
    ) -> DuckDBPyRelation:
        return self.con.sql(
            rf"SELECT * from read_parquet({[str(f) for f in self.path_stock_kline_1d_qfq.iterdir()]})"
        ).filter(
            f"(date >= TIMESTAMP '{start_date}') and (date < TIMESTAMP '{end_date}')"
        )

    def get_kline_1d_qfq_df(
        self, start_date: str = "2023-01-01", end_date: str = "2070-01-01"
    ) -> DataFrameType:
        return self.transform_to_df(self.get_kline_1d_qfq_rel(start_date, end_date))

    def update_kline_1d_qfq(self):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        """
        from pond.duckdb.stock.kline import get_kline_1d_qfq_df

        start_time = time.perf_counter()

        logger.info("Start to read local tdx stock 1d data ...")
        qfq_df = get_kline_1d_qfq_df(stock_basic_df=self.stock_basic_df, offset=1)
        filename = f'{qfq_df["date"].min().strftime("%Y%m%d")}_{qfq_df["date"].max().strftime("%Y%m%d")}'

        for f in self.path_stock_kline_1d_qfq.iterdir():
            if f.stem == filename:
                logger.info(
                    f"{filename}.parquet has been created, not save memory df to disk."
                )
                return
            else:
                # clear existing file
                f.unlink()

        logger.info("Start to write parquet file ...")
        (
            self.con.sql("select * from qfq_df").write_parquet(
                str(self.path_stock_kline_1d_qfq / f"{filename}.parquet"),
                compression=self.compress,
            )
        )

        logger.success(
            f"Update all parquet file cost: {time.perf_counter() - start_time:.2f}s"
        )

    def get_kline_1m(
        self, start_date: str = "2023-01-01", end_date: str = "2070-01-01"
    ) -> DuckDBPyRelation:
        rel = self.con.sql(
            rf"SELECT * from read_parquet({[str(f) for f in self.path_stock_kline_1m.iterdir()]})"
        ).filter(
            f"(date >= TIMESTAMP '{start_date}') and (date < TIMESTAMP '{end_date}')"
        )
        return self.transform_to_df(rel)

    def update_kline_1m_from_tdx(self, tdx_dir=None):
        from pond.akshare.stock.all_basic import get_all_stocks_df
        import os
        import math
        import ray
        from pond.tdx.kline import TdxReaderActor

        symbols = get_all_stocks_df()["代码"]
        process_counts = os.cpu_count() - 1
        readers = []
        group_size = math.ceil(len(symbols) / process_counts)

        for i in range(process_counts):
            reader = TdxReaderActor.remote(
                self.path_stock_kline_1m.absolute(), update=True, fetch_data=False
            )
            reader.read.remote(
                tdx_dir, "std", symbols[i * group_size : (i + 1) * group_size], "1"
            )
            readers.append(reader)

        for reader in readers:
            ray.get(reader.get.remote())


    def get_snapshot_1m_rel(
        self, start_date: str = "2023-01-01", end_date: str = "2070-01-01"
    ) -> DuckDBPyRelation:
        return self.con.sql(
            rf"SELECT * from read_parquet({[str(f) for f in self.path_stock_snapshot_1m.iterdir()]})"
        ).filter(
            f"(date >= TIMESTAMP '{start_date}') and (date < TIMESTAMP '{end_date}')"
        ) 


    def update_snapshot_1m(self, force=True):
        """
        aggrate 1m kline into market snapshot
        """
        def agg_into_snapshots(parquet_file) -> pl.DataFrame:
            df = pl.scan_parquet(parquet_file).with_columns([
                pl.col("date").alias("datetime"),
                pl.col("date").dt.date().alias("date"),
                pl.col("date").dt.time().alias("time"),
            ]).sort("datetime").with_columns([
                (pl.col("close") / pl.col("close").shift(1) -1).alias("chgpct")
            ]).with_columns([
                ((1 + pl.col("chgpct") * 10).log(2) * pl.col("amount")).alias("found"),#使用log函数模拟资金流入流出
            ])
            df_day = df.group_by("date").agg([
                pl.col("close").last().alias("dclose")
            ]).sort("date").with_columns([
                pl.col("dclose").shift(1).alias("dclose_lag1")
            ])
            df_minutes = []
            for time in pl.time_range(dtime(9,30), dtime(15,0), '1m', eager=True):
                if time > dtime(11,30) and time < dtime(13,0):
                    continue
                df_minute = df.filter(pl.col("time") <= time).group_by("date").agg([
                    pl.col("open").first(),
                    pl.col("high").max(),
                    pl.col("low").min(),
                    pl.col("close").last(),
                    pl.col("time").last(),
                    pl.col("datetime").last(),
                    pl.col("volume").sum(),
                    pl.col("amount").sum(),
                    pl.col("found").sum(),
                    pl.col("chgpct").last().alias("chgpct_1m")
                ]).with_columns([
                    pl.lit(parquet_file.stem).alias("code")
                ])
                df_minutes.append(df_minute)

            df : pl.LazyFrame = pl.concat(df_minutes)
            df = df.join(df_day, on='date')
            df = df.with_columns([
                (pl.col("close") / pl.col("dclose_lag1") -1).alias("dchgpct"),
                (pl.col("dclose") / pl.col("dclose_lag1") -1).alias("dchgpct_next0")
            ])
            return df.collect().sort("datetime")

        
        for file in self.path_stock_kline_1m.glob("*.parquet"):
            target = self.path_stock_snapshot_1m / file.name
            if target.exists() and not force:
                continue
            df = agg_into_snapshots(file)
            df.write_parquet(target, compression=self.compress.lower())
            print(f"writing {self.path_stock_snapshot_1m / file.name}")


    def update_indsutry_info(self):
        pindustry.update_industry_list(self.path_stock_industry)
        pindustry.update_industry_constituent(self.path_stock_industry)


    def read_industry_consituent(self) -> pl.LazyFrame:
        return pindustry.read_industry_consituent(self.path_stock_industry)
    

    def update_indsutry_index(self):
        pindustry.gen_industry_index(self.path_stock_industry)


    def read_industry_index(self) -> pl.LazyFrame:
        return pindustry.read_industry_index(self.path_stock_industry)


if __name__ == "__main1__":
    import time

    db = StockDB(Path(r"E:\DuckDB"))
    db.init_db_path()
    db.update_indsutry_index()
    #print(f"read indsutry {len(db.read_industry_consituent().collect())}")
    #db.update_indsutry_info()
    #print(f"read indsutry {len(db.read_industry_consituent().collect())}")

if __name__ == "__main__":
    import time
    db = StockDB(Path(r"E:\DuckDB"))
    db.init_db_path()
    # db = StockDB(Path(r'/home/fangyang/zhitai5000/DuckDB/'))
    # db.update_stock_orders()

    # rel = db.get_kline_1d_qfq_rel()
    # df = db.get_kline_1d_qfq_df().to_pandas()

    # db.update_stock_info()
    # db.update_kline_1d_nfq()
    # db.update_kline_1d_qfq()

    # db.update_level2_trade()
    # db.update_level2_order()
    # db.update_level2_orderbook()

    # r1 = db.con.sql(
    #     rf"SELECT * from read_parquet('{str(db.path_stock_trades / '20230504.parquet')}')")  # order by jj_code, datetime
    #
    # r4 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')")
    # r5 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'calender.parquet')}')")
    
    #db.update_kline_1m_from_tdx(r"D:\windows\programs\TongDaXin")

    db.update_snapshot_1m(force=False)

    start = time.perf_counter()
    df = db.get_kline_1m()
    print(f"fetched data {len(df)}, cost time {time.perf_counter() - start:.2f}")
    print(1)
