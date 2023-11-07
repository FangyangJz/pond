# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:00
# @Author   : Fangyang
# @Software : PyCharm
import time
from pathlib import Path

import pandas as pd
from loguru import logger

from pond.duckdb import DuckDB


class StockDB(DuckDB):

    def __init__(self, db_path: Path):
        self.path_stock = db_path / 'stock'
        self.path_stock_info = self.path_stock / 'info'
        self.path_stock_kline_1d = self.path_stock / 'kline_1d'
        self.path_stock_trades = self.path_stock / 'trades'
        self.path_stock_trades_origin = self.path_stock_trades / 'origin'
        self.path_stock_list = [
            self.path_stock, self.path_stock_info, self.path_stock_kline_1d,
            self.path_stock_trades, self.path_stock_trades_origin
        ]

        super().__init__(db_path)

    def init_db_path(self):
        [f.mkdir() for f in self.path_stock_list if not f.exists()]

    def update_stock_info(self):
        from pond.akshare.stock import get_all_stocks_df
        from akshare import tool_trade_date_hist_sina

        start_time = time.perf_counter()

        stock_basic_df = get_all_stocks_df()

        calender_df = tool_trade_date_hist_sina().astype('str')
        calender_df['trade_date'] = pd.to_datetime(calender_df['trade_date'])
        calender_df.reset_index(inplace=True)

        compress = 'ZSTD'

        (self.con.sql('select * from stock_basic_df')
         .write_parquet(str(self.path_stock_info / f'basic.parquet'), compression=compress))
        logger.success(f'Update basic.parquet cost: {time.perf_counter() - start_time}s')

        (self.con.sql('select * from calender_df')
         .write_parquet(str(self.path_stock_info / f'calender.parquet'), compression=compress))
        logger.success(f'Update calender.parquet cost: {time.perf_counter() - start_time}s')

    def update_stock_trades(self):
        from pond.duckdb.stock.trades import get_trade_df_with_multiprocess

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                trades_df = get_trade_df_with_multiprocess(dir_path)

                compress = 'ZSTD'
                start_time = time.perf_counter()
                (self.con.sql('select * from trades_df')
                 .write_parquet(str(self.path_stock_trades / f'{date_str}.parquet'), compression=compress))
                logger.success(f'Update {date_str}.parquet cost: {time.perf_counter() - start_time}s')


if __name__ == '__main__':
    db = StockDB(Path(r'D:\DuckDB'))
    db.update_stock_info()

    # db.update_stock_trades()
    r1 = db.con.sql(
        rf"SELECT * from read_parquet('{str(db.path_stock_trades / '20230504.parquet')}')")  # order by jj_code, datetime

    r4 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')")
    r5 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'calender.parquet')}')")
    print(1)
