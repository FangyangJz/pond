# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm

import time
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pond.duckdb import DuckDB


class CryptoDB(DuckDB):

    def __init__(self, db_path: Path):
        self.path_crypto = db_path / 'crypto'
        self.path_crypto_info = self.path_crypto / 'info'
        self.path_crypto_kline_1d = self.path_crypto / 'kline_1d'
        self.path_crypto_trades = self.path_crypto / 'trades'
        self.path_crypto_trades_origin = self.path_crypto_trades / 'origin'
        self.path_crypto_list = [
            self.path_crypto, self.path_crypto_info,
            self.path_crypto_kline_1d,
            self.path_crypto_trades, self.path_crypto_trades_origin
        ]

        super().__init__(db_path)

    def init_db_path(self):
        [f.mkdir() for f in self.path_crypto_list if not f.exists()]

    def update_crypto_trades(self):
        for f in (pbar := tqdm(self.path_crypto_trades_origin.glob('*.csv'))):
            pbar.set_postfix_str(str(f))
            (self.sql(
                f"SELECT id, price, qty, quote_qty, epoch_ms(time) as time, is_buyer_maker  "
                f"from read_csv_auto('{str(f)}') order by id")
             .write_parquet(str(self.path_crypto_trades / f'{f.stem}.parquet'), compression=self.compress))


if __name__ == '__main__':
    db = CryptoDB(Path(r'D:\DuckDB'))
    # db.update_crypto_trades()
