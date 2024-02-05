# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path
from typing import Dict, List

import pandas as pd
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

    def update_history_data(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        tz: TIMEZONE = "UTC",
        proxies: Dict[str, str] = {},
    ):
        from binance.cm_futures import CMFutures
        from binance.um_futures import UMFutures
        from pond.binance_history.api import fetch_data

        assert isinstance(asset_type, AssetType)
        assert isinstance(data_type, DataType)

        client = None

        if asset_type == AssetType.future_cm:
            client = CMFutures(proxies=proxies)
            if data_type == DataType.klines:
                base_path = self.path_crypto_kline_cm
            elif data_type == DataType.aggTrades:
                base_path = self.path_crypto_agg_trades

        elif asset_type == AssetType.future_um:
            client = UMFutures(proxies=proxies)
            if data_type == DataType.klines:
                base_path = self.path_crypto_kline_um
            elif data_type == DataType.aggTrades:
                base_path = self.path_crypto_agg_trades

        elif asset_type == AssetType.spot:
            raise NotImplemented("spot client has not been added")
            base_path = self.path_crypto_kline_spot

        symbol_list = self.get_local_future_perpetual_symbol_list(asset_type=asset_type)

        exist_files = [f.stem for f in (base_path / timeframe).glob("*.parquet")]

        for symbol in (
            pbar := tqdm(
                symbol_list,
                desc=f"Download {timeframe} {asset_type} data from {start} -> {end}",
            )
        ):
            if symbol in exist_files:
                logger.warning(f"{symbol} is existed, skip download.")
                continue

            pbar.set_postfix_str(f"{symbol}, download ...")
            data = fetch_data(
                symbol=symbol,
                asset_type=asset_type,
                data_type=data_type,
                start=start,
                end=end,
                timeframe=timeframe,
                tz=tz,
                local_path=self.path_crypto,
            )
            data["jj_code"] = symbol
            pbar.set_postfix_str(
                f"{symbol} download successfully, df shape: {data.shape}"
            )

            data.to_parquet(base_path / timeframe / f"{symbol}.parquet")

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

    db = CryptoDB(Path(r"E:\DuckDB"))
    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))

    db.update_future_info()

    # df = db.get_future_info(asset_type=AssetType.future_um)
    # ll = db.get_local_future_perpetual_symbol_list(asset_type=AssetType.future_um)

    db.update_history_data(
        start="2023-1-1",
        end="2024-2-1",
        asset_type=AssetType.future_um,
        data_type=DataType.klines,
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
