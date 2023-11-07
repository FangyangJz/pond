# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:00
# @Author   : Fangyang
# @Software : PyCharm
import os
import time
from multiprocessing import Pool, Manager
from multiprocessing.managers import ListProxy
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pond.duckdb import DuckDB


class StockDB(DuckDB):

    def __init__(self, db_path: Path):
        self.path_stock = db_path / 'stock'
        self.path_stock_info = self.path_stock / 'info'
        self.path_stock_kline_1d = self.path_stock / 'kline_1d'
        self.path_stock_kline_1d_nfq = self.path_stock_kline_1d / 'nfq'
        self.path_stock_kline_1d_qfq = self.path_stock_kline_1d / 'qfq'
        self.path_stock_trades = self.path_stock / 'trades'
        self.path_stock_trades_origin = self.path_stock_trades / 'origin'
        self.path_stock_list = [
            self.path_stock, self.path_stock_info,
            self.path_stock_kline_1d, self.path_stock_kline_1d_nfq, self.path_stock_kline_1d_qfq,
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

        (self.con.sql('select * from stock_basic_df')
         .write_parquet(str(self.path_stock_info / f'basic.parquet'), compression=self.compress))
        logger.success(f'Update basic.parquet cost: {time.perf_counter() - start_time}s')

        (self.con.sql('select * from calender_df')
         .write_parquet(str(self.path_stock_info / f'calender.parquet'), compression=self.compress))
        logger.success(f'Update calender.parquet cost: {time.perf_counter() - start_time}s')

    def update_stock_trades(self):
        from pond.duckdb.stock.trades import get_trade_df_with_multiprocess

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                trades_df = get_trade_df_with_multiprocess(dir_path)

                start_time = time.perf_counter()
                (self.con.sql('select * from trades_df')
                 .write_parquet(str(self.path_stock_trades / f'{date_str}.parquet'), compression=self.compress))
                logger.success(f'Update {date_str}.parquet cost: {time.perf_counter() - start_time}s')

    def update_kline_1d_nfq(self, offset: int = 0):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
        :return:
        """

        start_time = time.perf_counter()

        stock_basic_df = (
            self.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')").df())

        logger.info('Start to read local tdx stock 1d data ...')

        pool = Pool(os.cpu_count() - 1)
        step = int(len(stock_basic_df) / (4 * pool._processes))  # tune coe 4 get best speed
        _manager = Manager()
        res_list = _manager.list()

        pbar1 = tqdm(total=int(len(stock_basic_df) / step), position=0, leave=True)
        pbar1.set_description(
            f'Function update_kline_1d_nfq with multiprocess, total {len(stock_basic_df)}, step {step}')

        for i in range(0, len(stock_basic_df), step):
            pool.apply_async(
                update_res_list,
                args=(stock_basic_df.iloc[i:i + step], offset, res_list),
                callback=lambda *args: pbar1.update()
            )
        pool.close()
        pool.join()
        pbar1.close()

        concat_start_time = time.perf_counter()
        logger.info('Start to concat all stocks history dataframe ...')
        df = pd.concat(res_list).reset_index(names=['trade_date'])
        logger.success(f'Concat all stocks history dataframe cost: {time.perf_counter()-concat_start_time:.2f}s')

        logger.info('Start to write parquet file by date ...')
        for idx, group_df in (pbar2 := tqdm(df.groupby('trade_date'), position=0, leave=True)):
            file_name = str(idx.date()).replace('-', '')
            pbar2.set_postfix_str(file_name)

            (self.con.sql('select * from group_df')
             .write_parquet(str(self.path_stock_kline_1d_nfq / f'{file_name}.parquet'), compression=self.compress))

        pbar2.close()
        logger.success(f'Update all parquet file cost: {time.perf_counter() - start_time:.2f}s')


def update_res_list(stock_basic_slice_df: pd.DataFrame, offset: int, global_res_list: ListProxy):
    from mootdx.reader import Reader
    reader = Reader.factory(market='std', tdxdir='C:/new_tdx')

    # TODO multiprocess accelerate
    # for idx, row in (pbar := tqdm(stock_basic_slice_df.iterrows())):
    for idx, row in stock_basic_slice_df.iterrows():
        stock_code = row['代码']
        # pbar.set_postfix_str(f"{row['jj_code']} - {row['名称']} - {row['所处行业']}")

        stock_daily_df = reader.daily(symbol=stock_code)
        stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
        if stock_daily_df.empty:
            continue

        stock_daily_df['jj_code'] = row['jj_code']
        stock_daily_df['name'] = row['名称']
        stock_daily_df['industry'] = row['所处行业']

        global_res_list.append(stock_daily_df)


if __name__ == '__main__':
    db = StockDB(Path(r'D:\DuckDB'))
    db.update_kline_1d_nfq()
    # db.update_stock_info()

    # db.update_stock_trades()
    r1 = db.con.sql(
        rf"SELECT * from read_parquet('{str(db.path_stock_trades / '20230504.parquet')}')")  # order by jj_code, datetime

    r4 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')")
    r5 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'calender.parquet')}')")
    print(1)
