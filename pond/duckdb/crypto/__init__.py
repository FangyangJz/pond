# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path

import pandas as pd
from tqdm import tqdm
from loguru import logger

from pond.duckdb import DuckDB, DataFrameStrType, df_types
from pond.duckdb.crypto.custom_types import TIMEFRAMES, TIMEZONE, ASSET_TYPE


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
        self.path_crypto_orderbook = self.path_crypto / "orderbook"

        self.path_crypto_list = [
            self.path_crypto,
            self.path_crypto_data,
            self.path_crypto_info,
            self.path_crypto_kline,
            *self.get_common_interval_path_list(self.path_crypto_kline),
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

    def get_local_cm_future_perpetual_symbol_list(self):
        df = self.get_future_info()
        df = df[df['contractType'] == "PERPETUAL"]
        return df.symbol.to_list()

    def get_future_info(self, from_local=True):
        if from_local:
            file = self.path_crypto_info/'info.csv'
            if file.exists():
                info_df = pd.read_csv(file)
            else:
                logger.warning(f'{file} is not exist on local, start to read from network.')
                info_df = self.update_future_info()
        else:
            info_df = self.update_future_info()

        return info_df

    def update_future_info(self):
        file  = self.path_crypto_info/'info.csv'
        logger.info(f'Update {file} from network...')

        from binance.cm_futures import CMFutures
        from pond.duckdb.crypto.future import get_cm_future_info_df

        proxies = {
            "https": "127.0.0.1:7890",
        }
        client_cm_future = CMFutures(proxies=proxies)
        info_df = get_cm_future_info_df(client_cm_future)
        info_df.to_csv(self.path_crypto_info/'info.csv', index_label=False)
        
        return info_df

    def update_kline_cm_future(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: ASSET_TYPE = "futures/cm",
        timeframe: TIMEFRAMES = "1m",
        tz: TIMEZONE = "UTC",
    ):
        from binance.cm_futures import CMFutures
        from pond.binance_history.api import fetch_klines
        from pond.duckdb.crypto.future import get_cm_future_symbol_list

        proxies = {
            "https": "127.0.0.1:7890",
        }
        client_cm_future = CMFutures(proxies=proxies)
        # client_cm_future = CMFutures()

        cm_symbol_list = get_cm_future_symbol_list(client_cm_future)

        exist_files = [
            f.stem for f in (self.path_crypto_kline / timeframe).glob("*.parquet")
        ]

        for symbol in (
            pbar := tqdm(
                cm_symbol_list,
                desc=f"Download {timeframe} {asset_type} data from {start} -> {end}",
            )
        ):
            if symbol in exist_files:
                logger.warning(f"{symbol} is existed, skip download.")
                continue

            klines = fetch_klines(
                symbol=symbol,
                start=start,
                end=end,
                timeframe=timeframe,
                tz=tz,
                asset_type=asset_type,
                local_path=self.path_crypto,
            )
            klines["jj_code"] = symbol
            pbar.set_postfix_str(f"{symbol}, df shape: {klines.shape}")

            klines.to_parquet(self.path_crypto_kline / timeframe / f"{symbol}.parquet")

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
    df = db.get_future_info()
    ll = db.get_local_cm_future_perpetual_symbol_list()

    db.update_kline_cm_future()
    df = pl.read_parquet(
        db.path_crypto_kline / "1m" / "BTCUSD_PERP.parquet"
    ).to_pandas()
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
