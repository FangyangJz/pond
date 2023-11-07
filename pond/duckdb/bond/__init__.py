# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/8 1:33
# @Author   : Fangyang
# @Software : PyCharm

import time
from pathlib import Path

import pandas as pd
from loguru import logger

from pond.duckdb import DuckDB


class BondDB(DuckDB):
    def __init__(self, db_path: Path):
        self.path_bond = db_path / 'bond'
        self.path_bond_info = self.path_bond / 'info'
        self.path_bond_kline_1d = self.path_bond / 'kline_1d'
        self.path_bond_list = [self.path_bond, self.path_bond_info, self.path_bond_kline_1d]

        super().__init__(db_path)

    def init_db_path(self):
        [f.mkdir() for f in self.path_bond_list if not f.exists()]

    def update_bond_info(self):
        from pond.akshare.bond import get_bond_basic_df

        start_time = time.perf_counter()
        bond_basic_df, bond_redeem_df = get_bond_basic_df()
        bond_basic_df['上市时间'] = pd.to_datetime(bond_basic_df['上市时间'])
        bond_basic_df['转股起始日'] = pd.to_datetime(bond_basic_df['转股起始日'])
        bond_redeem_df['转股起始日'] = pd.to_datetime(bond_redeem_df['转股起始日'])

        compress = 'ZSTD'

        (self.con.sql('select * from bond_basic_df')
         .write_parquet(str(self.path_bond_info / f'basic.parquet'), compression=compress))
        logger.success(f'Update basic.parquet cost: {time.perf_counter() - start_time}s')

        (self.con.sql('select * from bond_redeem_df')
         .write_parquet(str(self.path_bond_info / f'redeem.parquet'), compression=compress))
        logger.success(f'Update redeem.parquet cost: {time.perf_counter() - start_time}s')


if __name__ == '__main__':
    db = BondDB(Path(r'D:\DuckDB'))
    db.update_bond_info()
    r2 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_bond_info / 'basic.parquet')}')")
    r3 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_bond_info / 'redeem.parquet')}')")
    print(1)
