# !/usr/bin/env python3
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm
import time
from pathlib import Path
import polars as pl
import pandas as pd
from tqdm import tqdm
from loguru import logger
from dateutil import parser, tz
from httpx._types import ProxiesTypes
from datetime import datetime
from binance.spot import Spot
from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures

from pond.duckdb import DuckDB, DataFrameStrType, df_types
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType


class CryptoDB(DuckDB):
    def __init__(self, db_path: Path, df_type: DataFrameStrType = df_types.polars):
        self.path_crypto = db_path / "crypto"
        self.path_crypto_data = self.path_crypto / "data"
        self.path_crypto_info = self.path_crypto / "info"

        self.path_crypto_kline = self.path_crypto / "kline"
        self.path_crypto_kline_spot = self.path_crypto_kline / "spot"
        self.path_crypto_kline_um = self.path_crypto_kline / "um"
        self.path_crypto_kline_cm = self.path_crypto_kline / "cm"
        self.path_crypto_trades = self.path_crypto / "trades"
        self.path_crypto_trades_origin = self.path_crypto_trades / "origin"

        self.path_crypto_agg_trades = self.path_crypto / "agg_trades"
        self.path_crypto_agg_trades_origin = self.path_crypto_agg_trades / "origin"
        self.path_crypto_agg_trades_spot = self.path_crypto_agg_trades / "spot"
        self.path_crypto_agg_trades_um = self.path_crypto_agg_trades / "um"
        self.path_crypto_agg_trades_cm = self.path_crypto_agg_trades / "cm"

        self.path_crypto_orderbook = self.path_crypto / "orderbook"

        self.path_crypto_list = [
            self.path_crypto,
            self.path_crypto_data,
            self.path_crypto_info,
            self.path_crypto_kline,
            self.path_crypto_kline_spot,
            *self.get_common_interval_path_list(self.path_crypto_kline_spot),
            self.path_crypto_kline_um,
            *self.get_common_interval_path_list(self.path_crypto_kline_um),
            self.path_crypto_kline_cm,
            *self.get_common_interval_path_list(self.path_crypto_kline_cm),
            self.path_crypto_trades,
            self.path_crypto_trades_origin,
            self.path_crypto_agg_trades,
            self.path_crypto_agg_trades_origin,
            self.path_crypto_agg_trades_spot,
            self.path_crypto_agg_trades_um,
            self.path_crypto_agg_trades_cm,
            self.path_crypto_orderbook,
        ]

        super().__init__(db_path, df_type)

    @staticmethod
    def get_common_interval_path_list(base_path: Path):
        return [base_path / freq for freq in ["1m", "1h", "1d"]]

    def init_db_path(self):
        [f.mkdir() for f in self.path_crypto_list if not f.exists()]

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
            file = self.path_crypto_info / f"{a2.upper()}{a1.capitalize()}.csv"
        else:
            file = self.path_crypto_info / f"{asset_type.value.capitalize()}.csv"

        if from_local:
            if file.exists():
                return pd.read_csv(file)

        self.update_future_info()

        return pd.read_csv(file)

    def update_future_info(self, proxies: dict[str, str] = {"https": "127.0.0.1:7890"}):
        """
        字段说明: https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7

        params: proxies={"https": "127.0.0.1:7890"}
        """
        from pond.duckdb.crypto.future import get_future_info_df

        file = self.path_crypto_info / "info.csv"
        logger.info(f"Update {file} from network...")

        for c in [
            CMFutures(proxies=proxies),
            UMFutures(proxies=proxies),
            Spot(proxies=proxies),
        ]:
            info_df = get_future_info_df(c)
            info_df.to_csv(
                self.path_crypto_info / f"{c.__class__.__name__}.csv", index_label=False
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

    def get_base_path(self, asset_type: AssetType, data_type: DataType) -> Path:
        return {
            (AssetType.spot, DataType.klines): self.path_crypto_kline_spot,
            (AssetType.spot, DataType.aggTrades): self.path_crypto_agg_trades_spot,
            (AssetType.future_cm, DataType.klines): self.path_crypto_kline_cm,
            (AssetType.future_cm, DataType.aggTrades): self.path_crypto_agg_trades_cm,
            (AssetType.future_um, DataType.klines): self.path_crypto_kline_um,
            (AssetType.future_um, DataType.aggTrades): self.path_crypto_agg_trades_um,
        }[(asset_type, data_type)]  # type: ignore

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
        requests_proxies: dict[str, str] = {"https": "127.0.0.1:7890"},
        skip_symbols: list[str] = [],
        ignore_cache=False,
        workers=None,
    ):
        from threading import Thread
        import math
        import os

        if workers is None:
            workers = int(os.cpu_count() / 2)

        df = self.get_future_info(asset_type)
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
        else:
            df = df[["symbol"]]

        task_size = math.ceil(len(df) / workers)
        threads = []
        for i in range(workers):
            t = Thread(
                target=self.update_history_data,
                args=(
                    df[i * task_size : (i + 1) * task_size],
                    start,
                    end,
                    asset_type,
                    data_type,
                    timeframe,
                    httpx_proxies,
                    requests_proxies,
                    skip_symbols,
                    ignore_cache,
                    i
                ),
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    def update_history_data(
        self,
        asset_info_df: pd.DataFrame,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        httpx_proxies: ProxiesTypes = {},
        requests_proxies: dict[str, str] = {"https": "127.0.0.1:7890"},
        skip_symbols: list[str] = [],
        ignore_cache=False,
        worker_id:int=0
    ):
        assert isinstance(asset_type, AssetType)
        assert isinstance(data_type, DataType)

        base_path = self.get_base_path(asset_type, data_type)
        exist_files = [f.stem for f in (base_path / timeframe).glob("*.parquet")]

        input_start = parser.parse(start).replace(tzinfo=tz.tzutc())
        input_end = parser.parse(end).replace(tzinfo=tz.tzutc())
        total_len = len(asset_info_df)

        for idx, row in (pbar := tqdm(asset_info_df.iterrows(), position=worker_id)):
            symbol = row["symbol"]

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
                continue
            df = self.load_history_data(
                symbol,
                _start,
                _end,
                asset_type,
                data_type,
                timeframe,
                httpx_proxies,
                requests_proxies,
            )
            if len(df) == 0:
                logger.warning(f"{symbol} load df is empty.")
            df.write_parquet(base_path / timeframe / f"{symbol}.parquet")

            pbar.set_description_str(f"[Worker_{worker_id}] Total {total_len}", refresh=False)
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
        httpx_proxies: ProxiesTypes = {},
        requests_proxies: dict[str, str] = {"https": "127.0.0.1:7890"},
    ) -> pl.DataFrame:
        from pond.binance_history.utils import (
            get_urls_by_xml_parse,
            load_data_from_disk,
        )
        from pond.binance_history.async_api import start_async_download_files
        from pond.duckdb.crypto.const import kline_schema
        from pond.duckdb.crypto.future import get_supply_df

        load_urls, download_urls = get_urls_by_xml_parse(
            data_type=data_type,
            asset_type=asset_type,
            symbol=symbol,
            start=start,
            end=end,
            timeframe=timeframe,
            file_path=self.path_crypto,
            proxies=requests_proxies,
        )

        start_async_download_files(
            download_urls, self.path_crypto, proxies=httpx_proxies
        )

        df_list = []
        for url in load_urls:
            df = load_data_from_disk(
                url,
                self.path_crypto,
                dtypes=kline_schema,
            )
            if df is not None:
                df_list.append(df)

        if df_list:
            df = pl.concat(df_list)
            df = self.filter_quote_volume_0(df, symbol, timeframe)
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
                ["open_time", "close_time"]
            )
            if len(lack_df) > 0:
                if len(lack_df) % 2 != 0:
                    lack_df = pl.concat(
                        [lack_df, df.select(["open_time", "close_time"])[-1]]
                    )

                supply_df = get_supply_df(
                    client=self.get_client(asset_type, requests_proxies),
                    lack_df=lack_df,
                    symbol=symbol,
                    interval=timeframe,
                )
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
                .unique(maintain_order=True)
            )

            logger.success(
                f"[{symbol}] Dataframe shape: {df.shape}, {df['open_time'].min()} -> {df['close_time'].max()}."
            )

        else:
            df = (
                pl.DataFrame({}, schema=kline_schema)
                .with_columns(jj_code=pl.lit(symbol))
                .select(pl.exclude("ignore"))
            )
        return df

    def update_crypto_trades(self):
        trades_list = [f.stem for f in self.path_crypto_trades.iterdir()]
        names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]

        for f in (pbar := tqdm(self.path_crypto_trades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in trades_list:
                (
                    self.con.sql(
                        f"SELECT id, price, qty, quote_qty, epoch_ms(time) as time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names}) order by id"
                    ).write_parquet(
                        str(self.path_crypto_trades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )

    def update_crypto_agg_trades(self):
        agg_trades_list = [f.stem for f in self.path_crypto_agg_trades.iterdir()]
        names = [
            "agg_trade_id",
            "price",
            "qty",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ]
        for f in (pbar := tqdm(self.path_crypto_agg_trades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in agg_trades_list:
                (
                    self.con.sql(
                        f"SELECT agg_trade_id, price, qty, first_trade_id, last_trade_id, "
                        f"epoch_ms(transact_time) as transact_time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names})"
                    ).write_parquet(
                        str(self.path_crypto_agg_trades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )


if __name__ == "__main__":
    db = CryptoDB(Path(r"E:\DuckDB"))
    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))

    db.update_future_info()

    # df = db.get_future_info(asset_type=AssetType.future_um)
    # ll = db.get_local_future_perpetual_symbol_list(asset_type=AssetType.future_um)

    def try_update_data(interval) -> bool:
        try:
            db.update_history_data_parallel(
                start="2020-1-1",
                end="2024-4-22",
                asset_type=AssetType.future_um,
                data_type=DataType.klines,
                timeframe=interval,
                # httpx_proxies={"https://": "https://127.0.0.1:7890"},
                requests_proxies={
                    "http": "127.0.0.1:7890",
                    "https": "127.0.0.1:7890",
                },
                ignore_cache=True,
                workers=12,
            )
        except BaseException as e:
            print(e)
            return False
        return True

    # ...start downloading...
    interval = "1h"
    complete = False
    retry = 0
    start_time = time.perf_counter()
    while not complete:
        complete = try_update_data(interval)
        retry += 1
        if retry < 1:
            continue
        else:
            break
    print(f"complete: {complete}, retried: {retry}, time cost: {time.perf_counter() - start_time:.2f}s")

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
