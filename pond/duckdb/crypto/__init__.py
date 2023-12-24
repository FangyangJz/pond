# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path

import pandas as pd
from tqdm import tqdm

from binance.cm_futures import CMFutures
from pond.duckdb import DuckDB, DataFrameStrType, df_types


class CryptoDB(DuckDB):
    def __init__(self, db_path: Path, df_type: DataFrameStrType = df_types.polars):
        self.path_crypto = db_path / "crypto"
        self.path_crypto_data = self.path_crypto / "data"
        self.path_crypto_info = self.path_crypto / "info"
        self.path_crypto_kline = self.path_crypto / "kline"
        self.path_crypto_trades = self.path_crypto / "trades"
        self.path_crypto_trades_origin = self.path_crypto_trades / "origin"
        self.path_crypto_agg_trades = self.path_crypto / "agg_trades"
        self.path_crypto_agg_trades_origin = self.path_crypto_agg_trades / "origin"
        # self.proxies = {
        #     "https": "127.0.0.1:7890",
        # }
        self.client_cm_future = CMFutures(
            # proxies=self.proxies
        )

        self.path_crypto_list = [
            self.path_crypto,
            self.path_crypto_data,
            self.path_crypto_info,
            self.path_crypto_kline,
            self.path_crypto_trades,
            self.path_crypto_trades_origin,
            self.path_crypto_agg_trades,
            self.path_crypto_agg_trades_origin,
        ]

        super().__init__(db_path, df_type)

    def init_db_path(self):
        [f.mkdir() for f in self.path_crypto_list if not f.exists()]

    def update_kline_cm_future(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: str = "futures/cm",
        timeframe: str = "1m",
        tz="Asia/Shanghai",
    ):
        from pond.binance_history.api import fetch_klines
        from pond.duckdb.crypto.future import get_cm_future_symbol_list

        cm_symbol_list = get_cm_future_symbol_list(self.client_cm_future)

        kline_df_list = []
        for symbol in (pbar := tqdm(cm_symbol_list)):
            klines = fetch_klines(
                symbol=symbol,
                start=start,
                end=end,
                timeframe=timeframe,
                tz=tz,
                asset_type=asset_type,
            )
            pbar.set_postfix_str(f"{symbol}, df shape: {klines.shape}")
            kline_df_list.append(klines)

        kline_df = pd.concat(kline_df_list)
        kline_df.to_parquet(
            self.path_crypto_kline
            / f'{asset_type.replace("/", "_") if "/" in asset_type else asset_type}_{timeframe}.parquet'
        )

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

    # db.update_kline_cm_future()
    df = pl.read_parquet(db.path_crypto_kline/'futures_cm_1m.parquet').to_pandas()
    print(1)

    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))
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
