# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path
from typing import Dict, List

import pandas as pd
import datetime as dt
from dateutil import parser, tz
from tqdm import tqdm
from loguru import logger
from pond.duckdb import DuckDB, DataFrameStrType, df_types
from pond.binance_history.type import TIMEFRAMES, TIMEZONE, AssetType, DataType


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
        return [base_path / freq for freq in ["1m", "1d"]]

    def init_db_path(self):
        [f.mkdir() for f in self.path_crypto_list if not f.exists()]

    def get_local_future_perpetual_symbol_list(
        self, asset_type: AssetType
    ) -> List[str]:
        df = self.get_future_info(asset_type)
        df = df[df["contractType"] == "PERPETUAL"]
        return df.symbol.to_list()

    def get_future_info(self, asset_type: AssetType, from_local=True) -> pd.DataFrame:
        a1, a2 = asset_type.value.split("/")
        file = self.path_crypto_info / f"{a2.upper()}{a1.capitalize()}.csv"

        if from_local:
            if file.exists():
                return pd.read_csv(file)

        self.update_future_info()

        return pd.read_csv(file)

    def update_future_info(self, proxies: Dict[str, str] = {"https": "127.0.0.1:7890"}):
        """
        字段说明: https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7

        params: proxies={"https": "127.0.0.1:7890"}
        """
        file = self.path_crypto_info / "info.csv"
        logger.info(f"Update {file} from network...")

        from binance.cm_futures import CMFutures
        from binance.um_futures import UMFutures
        from pond.duckdb.crypto.future import get_future_info_df

        clients = [CMFutures(proxies=proxies), UMFutures(proxies=proxies)]

        for c in clients:
            info_df = get_future_info_df(c)
            info_df.to_csv(
                self.path_crypto_info / f"{c.__class__.__name__}.csv", index_label=False
            )

    def get_client(self, asset_type: AssetType, proxies: Dict[str, str] = {}):
        if asset_type == AssetType.future_cm:
            from binance.cm_futures import CMFutures

            return CMFutures(proxies=proxies)

        elif asset_type == AssetType.future_um:
            from binance.um_futures import UMFutures

            return UMFutures(proxies=proxies)

        elif asset_type == AssetType.spot:
            raise NotImplemented("spot client has not been added")

    def get_base_path(self, asset_type: AssetType, data_type: DataType) -> Path:
        path_map = {
            (AssetType.spot, DataType.klines): self.path_crypto_kline_spot,
            (AssetType.spot, DataType.aggTrades): self.path_crypto_agg_trades_spot,
            (AssetType.future_cm, DataType.klines): self.path_crypto_kline_cm,
            (AssetType.future_cm, DataType.aggTrades): self.path_crypto_agg_trades_cm,
            (AssetType.future_um, DataType.klines): self.path_crypto_kline_um,
            (AssetType.future_um, DataType.aggTrades): self.path_crypto_agg_trades_um,
        }

        return path_map[(asset_type, data_type)]

    def update_history_data(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        timezone: TIMEZONE = "UTC",
        proxies: Dict[str, str] = {},
        skip_symbols: List[str] = [],
    ):
        from pond.binance_history.utils import (
            get_urls,
            load_data_from_disk,
            get_local_data_path,
        )
        from pond.binance_history.async_api import start_async_download_files

        assert isinstance(asset_type, AssetType)
        assert isinstance(data_type, DataType)

        base_path = self.get_base_path(asset_type, data_type)

        df = self.get_future_info(asset_type)
        df = df[df["contractType"] == "PERPETUAL"][
            ["symbol", "contractType", "deliveryDate", "onboardDate", "update_datetime"]
        ]

        # symbol_list = self.get_local_future_perpetual_symbol_list(asset_type=asset_type)
        exist_files = [f.stem for f in (base_path / timeframe).glob("*.parquet")]

        input_start = parser.parse(start).replace(tzinfo=tz.tzutc())
        input_end = parser.parse(end).replace(tzinfo=tz.tzutc())
        total_len = len(df)

        for idx, row in (pbar := tqdm(df.iterrows())):
            symbol = row["symbol"]
            delivery_date = parser.parse(row["deliveryDate"])
            # TUSDT onboardDate 2023-01-31, but real history data is 2023-02-01
            onboard_date = parser.parse(row["onboardDate"]) + dt.timedelta(days=1)
            _start = max(onboard_date, input_start)
            _end = min(delivery_date, input_end)

            if _start > _end:
                logger.warning(
                    f"{symbol} start:{_start} exceed end:{_end}, skip download."
                )
                continue

            if symbol in exist_files:
                logger.warning(f"{symbol} is existed, skip download.")
                continue

            if symbol in skip_symbols:
                continue

            pbar.set_description_str(f"Total {total_len}", refresh=False)
            pbar.set_postfix_str(
                f"{symbol}, download {timeframe} {asset_type.value} data from {_start} -> {_end} ..."
            )

            load_urls, download_urls = get_urls(
                data_type=data_type,
                asset_type=asset_type,
                symbol=symbol,
                start=_start,
                end=_end,
                timeframe=timeframe,
                file_path=self.path_crypto,
            )

            start_async_download_files(download_urls, self.path_crypto)

            df_list = []
            for url in load_urls:
                df = load_data_from_disk(
                    url,
                    self.path_crypto,
                    dtypes={
                        "open_time": pl.Int64,
                        "open": pl.Float64,
                        "high": pl.Float64,
                        "low": pl.Float64,
                        "close": pl.Float64,
                        "volume": pl.Float64,
                        "close_time": pl.Int64,
                        "quote_volume": pl.Float64,
                        "count": pl.Int64,
                        "taker_buy_volume": pl.Float64,
                        "taker_buy_quote_volume": pl.Float64,
                        "ignore": pl.Int8,
                    },
                )

                if df is not None:
                    df_list.append(df)

            if df_list:
                df = (
                    pl.concat(df_list)
                    .with_columns(
                        (pl.col("open_time") * 1e3).cast(pl.Datetime),
                        (pl.col("close_time") * 1e3).cast(pl.Datetime),
                        jj_code=pl.lit(symbol),
                    )
                    .select(pl.exclude("ignore"))
                )
                logger.success(f"{symbol} load df shape: {df.shape}")
                df.write_parquet(base_path / timeframe / f"{symbol}.parquet")
            else:
                logger.warning(f"{symbol} load df is None")

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
    import polars as pl
    from pond.binance_history.exceptions import DataNotFound

    db = CryptoDB(Path(r"E:\DuckDB"))
    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))

    # db.update_future_info()

    # df = db.get_future_info(asset_type=AssetType.future_um)
    # ll = db.get_local_future_perpetual_symbol_list(asset_type=AssetType.future_um)

    db.update_history_data(
        start="2022-1-1",
        end="2024-2-10",
        asset_type=AssetType.future_um,
        data_type=DataType.klines,
        # proxies={"https://": "https://127.0.0.1:7890"},
    )

    df = pl.read_parquet(db.path_crypto_kline_um / "1m" / "BTCUSDT.parquet").to_pandas()
    print(1)

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
