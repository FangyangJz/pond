# !/usr/bin/env python3
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm
import os
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import polars as pl
import pandas as pd
from tqdm import tqdm
from loguru import logger
from dateutil import parser, tz
from httpx._types import ProxiesTypes
from binance.spot import Spot
from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures

from pond.duckdb import DuckDB, DataFrameStrType, df_types
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType
from pond.duckdb.crypto.const import timeframe_data_types_dict
from pond.duckdb.crypto.path import CryptoPath


class CryptoDB(DuckDB):
    def __init__(
        self,
        db_path: Path,
        requests_proxies: dict[str, str] = {},
        df_type: DataFrameStrType = df_types.polars,
    ):
        self.crypto_path = CryptoPath(crypto_path=db_path / "crypto")
        self.init_db_path = self.crypto_path.init_db_path
        self.requests_proxies = requests_proxies
        super().__init__(db_path, df_type)

    def get_local_future_perpetual_symbol_list(
        self, asset_type: AssetType
    ) -> list[str]:
        df = self.get_future_info(asset_type)
        df = df[df["contractType"] == "PERPETUAL"]
        return df.symbol.to_list()

    @staticmethod
    def is_future_type(asset_type: AssetType):
        return asset_type in [AssetType.future_um, AssetType.future_cm]

    def get_future_info(self, asset_type: AssetType, from_local=True) -> pd.DataFrame:
        if self.is_future_type(asset_type):
            a1, a2 = asset_type.value.split("/")
            file = self.crypto_path.info / f"{a2.upper()}{a1.capitalize()}.csv"
        else:
            file = self.crypto_path.info / f"{asset_type.value.capitalize()}.csv"

        if from_local:
            if file.exists():
                return pd.read_csv(file)

        self.update_future_info()

        return pd.read_csv(file)

    def update_future_info(self):
        """
        字段说明: https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7

        params: proxies={"https": "127.0.0.1:7890"}
        """
        from pond.duckdb.crypto.future import get_future_info_df

        file = self.crypto_path.info / "info.csv"
        logger.info(f"Update {file} from network...")

        for c in [
            CMFutures(proxies=self.requests_proxies),
            UMFutures(proxies=self.requests_proxies),
            Spot(proxies=self.requests_proxies),
        ]:
            info_df = get_future_info_df(c)
            info_df.to_csv(
                self.crypto_path.info / f"{c.__class__.__name__}.csv", index_label=False
            )

    def get_client(
        self, asset_type: AssetType, proxies: dict[str, str] = {}
    ) -> CMFutures | UMFutures | Spot:
        match asset_type:
            case AssetType.future_cm:
                return CMFutures(proxies=proxies)
            case AssetType.future_um:
                return UMFutures(proxies=proxies)
            case AssetType.spot:
                return Spot(proxies=proxies)
            case _:
                raise ValueError(f"{asset_type} is a wrong {AssetType}")

    @staticmethod
    def get_csv_schema(data_type: DataType) -> dict[str, pl.DataType]:
        from pond.duckdb.crypto.const import (
            klines_schema,
            metrics_schema,
            fundingRate_schema,
        )

        return {
            DataType.klines: klines_schema,
            DataType.metrics: metrics_schema,
            DataType.trades: {
                "id": pl.Int64,
                "price": pl.Float64,
                "qty": pl.Float64,
                "quoteQty": pl.Float64,
                "time": pl.Int64,
                "isBuyerMaker": pl.Boolean,
                "isBestMatch": pl.Boolean,
            },
            # DataType.aggTrades: {
            #     "a": pl.Int64,
            #     "p": pl.Float64,
            #     "q": pl.Float64,
            #     "f": pl.Int64,
            #     "l": pl.Int64,
            #     "T": pl.Int64,
            #     "m": pl.Boolean,
            #     "M": pl.Boolean,
            # },
            DataType.aggTrades: {
                "agg_trade_id": pl.Int64,
                "price": pl.Float64,
                "quantity": pl.Float64,
                "first_trade_id": pl.Int64,
                "last_trade_id": pl.Int64,
                "transact_time": pl.Int64,
                "is_buyer_maker": pl.Boolean,
            },
            DataType.fundingRate: fundingRate_schema,
        }[data_type]

    @staticmethod
    def filter_quote_volume_0(df: pl.DataFrame, symbol: str, timeframe: TIMEFRAMES):
        origin_df_len = len(df)
        df = (
            df.filter(pl.col("quote_volume") > 0)
            if timeframe not in ["1s", "1m"]
            else df
        )
        if (filtered_rows := origin_df_len - len(df)) > 0:
            logger.warning(f"[{symbol}] filter(quote_volume==0): {filtered_rows} rows.")

        return df

    def update_history_data_parallel(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        httpx_proxies: ProxiesTypes = {},
        skip_symbols: list[str] = [],
        do_filter_quote_volume_0: bool = False,
        if_skip_usdc: bool = True,
        ignore_cache=False,
        workers=None,
    ):
        df = self.get_future_info(asset_type, from_local=False)
        if self.is_future_type(asset_type):
            df = df[df["contractType"] == "PERPETUAL"][
                [
                    "symbol",
                    "contractType",
                    "deliveryDate",
                    "onboardDate",
                    "update_datetime",
                ]
            ]
            manual_df = pd.read_csv(Path(__file__).parent / "UMFutures_manual.csv")
            df = pd.concat([df, manual_df])
        else:
            # TODO spot 目前也走这里, 主要还是以合约标的为主
            df = self.get_future_info(AssetType.future_um, from_local=False)
            df = df[df["contractType"] == "PERPETUAL"][
                [
                    "symbol",
                    "contractType",
                    "deliveryDate",
                    "onboardDate",
                    "update_datetime",
                ]
            ]
            manual_df = pd.read_csv(Path(__file__).parent / "UMFutures_manual.csv")
            df = pd.concat([df, manual_df])

        df = df.sort_values(by="symbol")
        assert len(df) == len(set(df["symbol"].to_list())), (
            "symbol have duplicated data"
        )

        # 自动均分DataFrame，无需计算task_size
        chunks = np.array_split(df, workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    self.update_history_data,
                    chunk,
                    start,
                    end,
                    asset_type,
                    data_type,
                    timeframe,
                    httpx_proxies,
                    skip_symbols,
                    do_filter_quote_volume_0,
                    if_skip_usdc,
                    ignore_cache,
                    i,
                )
                for i, chunk in enumerate(chunks)
            ]

            # 等待所有任务完成
            for future in futures:
                future.result()

    def update_history_data(
        self,
        asset_info_df: pd.DataFrame,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        httpx_proxies: ProxiesTypes = {},
        skip_symbols: list[str] = [],
        do_filter_quote_volume_0: bool = False,
        if_skip_usdc: bool = True,
        ignore_cache=False,
        worker_id: int = 0,
    ):
        assert isinstance(asset_type, AssetType)
        assert isinstance(data_type, DataType)
        assert data_type in timeframe_data_types_dict[timeframe]

        base_path = self.crypto_path.get_base_path(asset_type, data_type)
        exist_files = [f.stem for f in (base_path / timeframe).glob("*.parquet")]

        input_start = parser.parse(start).replace(tzinfo=tz.tzutc())
        input_end = parser.parse(end).replace(tzinfo=tz.tzutc())
        total_len = len(asset_info_df)

        for idx, row in (pbar := tqdm(asset_info_df.iterrows(), position=worker_id)):
            symbol = row["symbol"]
            # if symbol != "CRVUSDT":
            #     continue

            if self.is_future_type(asset_type):
                delivery_date = parser.parse(row["deliveryDate"])
                # TUSDT onboardDate 2023-01-31, but real history data is 2023-02-01
                onboard_date = parser.parse(
                    row["onboardDate"]
                )  # + dt.timedelta(days=1)
                _start = max(onboard_date, input_start)
                _end = min(delivery_date, input_end)
            else:
                _start = input_start
                _end = input_end

            if _start > _end:
                logger.warning(
                    f"{symbol} start:{_start} exceed end:{_end}, skip download."
                )
                continue

            if symbol in exist_files and not ignore_cache:
                logger.warning(f"{symbol} is existed, skip download.")
                continue

            if symbol in skip_symbols:
                logger.warning(f"{symbol} in skip_symbols, skip download.")
                continue
            if if_skip_usdc and symbol.endswith("USDC"):
                logger.warning(f"{symbol} is USDC, skip download.")
                continue

            df = self.load_history_data(
                symbol,
                _start,
                _end,
                asset_type,
                data_type,
                timeframe,
                do_filter_quote_volume_0,
                httpx_proxies,
            )
            if data_type == DataType.klines:
                df = df.filter(pl.col("open_time") < _end.replace(tzinfo=None))
            elif data_type == DataType.fundingRate:
                raise ValueError("fundingRate not support filter end time")
            elif data_type == DataType.metrics:
                df = df.filter(pl.col("create_time") < _end.replace(tzinfo=None))

            if len(df) == 0:
                logger.warning(
                    f"{symbol} load df is empty, and skip save to parquet file"
                )
            else:
                if data_type == DataType.klines:
                    df.write_parquet(base_path / timeframe / f"{symbol}.parquet")
                elif data_type == DataType.metrics:
                    df.write_parquet(base_path / "5m" / f"{symbol}.parquet")
                elif data_type == DataType.fundingRate:
                    df.write_parquet(base_path / "8h" / f"{symbol}.parquet")

            pbar.set_description_str(
                f"[Worker_{worker_id}] Total {total_len}", refresh=False
            )
            pbar.set_postfix_str(
                f"{symbol}, download {timeframe} {asset_type.value} data from {_start} -> {_end} ..."
            )

    def load_history_data(
        self,
        symbol,
        start: datetime,
        end: datetime,
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        do_filter_quote_volume_0: bool = False,
        httpx_proxies: ProxiesTypes = {},
    ) -> pl.DataFrame:
        from pond.binance_history.utils import (
            get_urls_by_xml_parse,
            load_data_from_disk,
        )
        from pond.binance_history.async_api import start_async_download_files

        load_urls, download_urls = get_urls_by_xml_parse(
            data_type=data_type,
            asset_type=asset_type,
            symbol=symbol,
            start=start,
            end=end,
            timeframe=timeframe,
            file_path=self.crypto_path.crypto,
            proxies=self.requests_proxies,
        )

        start_async_download_files(
            download_urls, self.crypto_path.crypto, proxies=httpx_proxies
        )
        csv_schema = self.get_csv_schema(data_type)

        df_list = []
        if data_type == DataType.klines:
            for url in load_urls:
                df = load_data_from_disk(
                    url,
                    self.crypto_path.crypto,
                    dtypes=csv_schema,
                )
                if df is not None:
                    df_list.append(df)

        if df_list:
            df = pl.concat(df_list)
            if data_type == DataType.klines:
                df = self.transform_klines(
                    df,
                    symbol,
                    asset_type,
                    timeframe,
                    do_filter_quote_volume_0,
                )
            elif data_type == DataType.trades:
                pass
            elif data_type == DataType.aggTrades:
                pass
            elif data_type == DataType.metrics:
                df = self.transform_metrics(df)
            elif data_type == DataType.fundingRate:
                df = self.transform_fundingRate(df, symbol)
        else:
            df = (
                pl.DataFrame({}, schema=csv_schema)
                .with_columns(jj_code=pl.lit(symbol))
                .select(pl.exclude("ignore"))
            )

        return df

    def transform_fundingRate(self, df: pl.DataFrame, symbol: str):
        return df.with_columns(
            (pl.col("calc_time") * 1e3).cast(pl.Datetime),
            jj_code=pl.lit(symbol),
        )

    def transform_metrics(self, df: pl.DataFrame):
        return df.with_columns(
            pl.col("create_time")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .dt.truncate("5m")
        ).rename({"symbol": "jj_code"})

    def transform_klines(
        self,
        df: pl.DataFrame,
        symbol: str,
        asset_type: AssetType,
        timeframe: TIMEFRAMES,
        do_filter_quote_volume_0: bool = False,
    ):
        from pond.duckdb.crypto.future import get_supply_df

        if do_filter_quote_volume_0:
            df = self.filter_quote_volume_0(df, symbol, timeframe)

        # spot 数据这里有timestamp不一致的问题, 这里处理
        # 新增代码：处理 open_time 和 close_time 时间戳
        df = df.with_columns(
            pl.when(pl.col("open_time").cast(str).str.len_bytes() > 13)
            .then(pl.col("open_time").cast(str).str.slice(0, 13).cast(pl.Int64))
            .otherwise(pl.col("open_time"))
            .alias("open_time"),
            pl.when(pl.col("close_time").cast(str).str.len_bytes() > 13)
            .then(pl.col("close_time").cast(str).str.slice(0, 13).cast(pl.Int64))
            .otherwise(pl.col("close_time"))
            .alias("close_time"),
        )

        df = (
            df.sort("open_time").with_columns(
                (pl.col("open_time").diff() - pl.col("open_time").diff().shift(-1))
                .fill_null(0)
                .alias("open_time_diff"),
            )
            # for debug
            # .with_columns((pl.col("open_time") * 1e3).cast(pl.Datetime))
            # .to_pandas()
        )

        lack_df = df.filter(pl.col("open_time_diff") != 0).select(
            [
                "open_time",
                "close_time",
            ]
        )
        if len(lack_df) > 0:
            if len(lack_df) % 2 != 0:
                lack_df = pl.concat(
                    [
                        lack_df,
                        df.select(["open_time", "close_time"])[-1],
                    ]
                )

            supply_df = get_supply_df(
                client=self.get_client(asset_type, self.requests_proxies),
                lack_df=lack_df,
                symbol=symbol,
                interval=timeframe,
            )
            if do_filter_quote_volume_0:
                supply_df = self.filter_quote_volume_0(supply_df, symbol, timeframe)
            df = pl.concat([df.select(pl.exclude("open_time_diff")), supply_df])
        else:
            df = df.select(pl.exclude("open_time_diff"))

        df = (
            df.select(pl.exclude("ignore"))
            .with_columns(
                (pl.col("open_time") * 1e3).cast(pl.Datetime),
                (pl.col("close_time") * 1e3).cast(pl.Datetime),
                jj_code=pl.lit(symbol),
            )
            .sort("open_time")
            .fill_null(strategy="forward")
            .unique(maintain_order=True)
        )
        # check_df = df.to_pandas()
        logger.success(
            f"[{symbol}] Dataframe shape: {df.shape}, {df['open_time'].min()} -> {df['close_time'].max()}."
        )

        return df

    def update_crypto_trades(self):
        trades_list = [f.stem for f in self.crypto_path.trades.iterdir()]
        names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]

        for f in (pbar := tqdm(self.crypto_path.trades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in trades_list:
                (
                    self.con.sql(
                        f"SELECT id, price, qty, quote_qty, epoch_ms(time) as time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names}) order by id"
                    ).write_parquet(
                        str(self.crypto_path.trades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )

    def update_crypto_agg_trades(self):
        agg_trades_list = [f.stem for f in self.crypto_path.aggTrades.iterdir()]
        names = [
            "agg_trade_id",
            "price",
            "qty",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ]
        for f in (pbar := tqdm(self.crypto_path.aggTrades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in agg_trades_list:
                (
                    self.con.sql(
                        f"SELECT agg_trade_id, price, qty, first_trade_id, last_trade_id, "
                        f"epoch_ms(transact_time) as transact_time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names})"
                    ).write_parquet(
                        str(self.crypto_path.aggTrades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )

    def compare_um_future_info_with_vision(self):
        local_path = Path(r"E:\DuckDB\crypto\data\futures\um\monthly\klines")
        local_symbols = [i.stem for i in local_path.iterdir()]
        info_symbols = self.get_future_info(AssetType.future_um, from_local=False)[
            "symbol"
        ].to_list()
        diff_set = set(local_symbols) - set(info_symbols)
        print(diff_set)


if __name__ == "__main__":
    db = CryptoDB(
        Path(r"/home/fangyang/DuckDB/"),
        requests_proxies={
            "http": "127.0.0.1:7890",
            "https": "127.0.0.1:7890",
        },
    )

    # db.compare_um_future_info_with_vision()

    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))

    # db.update_future_info()

    # df = db.get_future_info(asset_type=AssetType.future_um)
    # ll = db.get_local_future_perpetual_symbol_list(asset_type=AssetType.future_um)

    db.update_history_data_parallel(
        start="2020-1-1",
        end="2025-8-3",
        asset_type=AssetType.spot,
        data_type=DataType.klines,
        timeframe="1h",
        # httpx_proxies={"https://": "https://127.0.0.1:7890"},
        skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT"],
        do_filter_quote_volume_0=False,
        if_skip_usdc=True,
        ignore_cache=False,
        workers=os.cpu_count() - 2,
    )

    def try_update_data(interval) -> bool:
        try:
            db.update_history_data_parallel(
                start="2020-1-1",
                end="2025-7-22",
                asset_type=AssetType.spot,
                data_type=DataType.klines,
                timeframe=interval,
                # httpx_proxies={"https://": "https://127.0.0.1:7890"},
                skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT"],
                do_filter_quote_volume_0=False,
                if_skip_usdc=True,
                ignore_cache=False,
                workers=os.cpu_count() - 2,
            )
        except BaseException as e:
            print(e)
            return False
        return True

    # ...start downloading...
    # interval = "1h"
    # complete = False
    # retry = 0
    # start_time = time.perf_counter()
    # while not complete:
    #     complete = try_update_data(interval)
    #     retry += 1
    #     if retry < 1:
    #         continue
    #     else:
    #         break
    # print(
    #     f"complete: {complete}, retried: {retry}, time cost: {time.perf_counter() - start_time:.2f}s"
    # )

    # db.update_crypto_trades()

    # for f in (pbar := tqdm(db.path_crypto_agg_trades_origin.glob("*.csv"))):
    #     r = db.con.read_csv(
    #         f,
    #         names=[
    #             "agg_trade_id",
    #             "price",
    #             "qty",
    #             "first_trade_id",
    #             "last_trade_id",
    #             "transact_time",
    #             "is_buyer_maker",
    #         ],
    #     )
    #     print(1)
    # db.update_crypto_agg_trades()
