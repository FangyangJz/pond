# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:00
# @Author   : Fangyang
# @Software : PyCharm

import time
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

    @property
    def stock_basic_df(self):
        return self.con.sql(rf"SELECT * from read_parquet('{str(self.path_stock_info / 'basic.parquet')}')").df()

    @property
    def calender_df(self):
        return self.con.sql(rf"SELECT * from read_parquet('{str(self.path_stock_info / 'calender.parquet')}')").df()

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

    def update_stock_orders(self):
        from pond.duckdb.stock.trades import get_order_df

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                files_list = list(dir_path.glob('*\逐笔委托.csv'))
                dd = self.con.sql(f'select * from read_csv("{str(files_list[0])}")')
                print(1)

    def update_stock_trades2(self):
        """
        No use duckdb read csv file directly because duckdb doesn't support GBK encoding.
        Need to read by pandas and then map to duckdb.
        """
        from pond.duckdb.stock.trades import get_level2_df_with_multiprocess

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                trades_df = get_level2_df_with_multiprocess(dir_path)

                start_time = time.perf_counter()
                (self.con.sql('select * from trades_df')
                 .write_parquet(str(self.path_stock_trades / f'{date_str}.parquet'), compression=self.compress))
                logger.success(f'Update {date_str}.parquet cost: {time.perf_counter() - start_time}s')

    def update_stock_trades(self):
        """
        No use duckdb read csv file directly because duckdb doesn't support GBK encoding.
        Need to read by pandas and then map to duckdb.
        """
        from pond.duckdb.stock.trades import get_level2_df_with_multiprocess, get_trade_script

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                df = get_level2_df_with_multiprocess(dir_path)

                start_time = time.perf_counter()
                (self.con.sql(get_trade_script())
                 .write_parquet(str(self.path_stock_trades / f'{date_str}.parquet'), compression=self.compress))
                logger.success(f'Update {date_str}.parquet cost: {time.perf_counter() - start_time:.4f}s')

    def update_kline_1d_nfq(self, offset: int = 0):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
        :return:
        """
        from pond.duckdb.stock.kline import get_kline_1d_nfq_df

        start_time = time.perf_counter()

        logger.info('Start to read local tdx stock 1d data ...')
        df = get_kline_1d_nfq_df(stock_basic_df=self.stock_basic_df, offset=offset)

        files = [f.stem for f in self.path_stock_kline_1d_nfq.iterdir()]
        logger.info('Start to write parquet file by date ...')
        for idx, group_df in (pbar2 := tqdm(df.groupby('trade_date'), position=0, leave=True)):
            file_name = str(idx.date()).replace('-', '')
            pbar2.set_postfix_str(file_name)

            if file_name not in files:
                (self.con.sql('select * from group_df')
                 .write_parquet(str(self.path_stock_kline_1d_nfq / f'{file_name}.parquet'), compression=self.compress))

        pbar2.close()
        logger.success(f'Update all parquet file cost: {time.perf_counter() - start_time:.2f}s')

    def get_kline_1d_qfq_df(self):

        return (
            self.con
            .sql(
                rf"SELECT * from read_parquet({[str(f) for f in self.path_stock_kline_1d_qfq.iterdir()]})"
            )
            .df()
        )

    def update_kline_1d_qfq(self):
        """
        读取本地数据, 写入db, mootdx reader 目前代码不涵盖 bj 路径, 故没有北交所数据
        """
        from pond.duckdb.stock.kline import get_kline_1d_qfq_df

        start_time = time.perf_counter()

        logger.info('Start to read local tdx stock 1d data ...')
        qfq_df = get_kline_1d_qfq_df(stock_basic_df=self.stock_basic_df, offset=1)
        filename = f'{qfq_df["date"].min().strftime("%Y%m%d")}_{qfq_df["date"].max().strftime("%Y%m%d")}'

        for f in self.path_stock_kline_1d_qfq.iterdir():
            if f.stem == filename:
                logger.info(f'{filename}.parquet has been created, not save memory df to disk.')
                return
            else:
                # clear existing file
                f.unlink()

        logger.info('Start to write parquet file ...')
        (self.con.sql('select * from qfq_df')
         .write_parquet(str(self.path_stock_kline_1d_qfq / f'{filename}.parquet'), compression=self.compress))

        logger.success(f'Update all parquet file cost: {time.perf_counter() - start_time:.2f}s')


if __name__ == '__main__':
    db = StockDB(Path(r'E:\DuckDB'))
    # db.update_stock_orders()

    # df = db.get_kline_1d_qfq_df()

    # db.update_stock_info()
    # db.update_kline_1d_nfq()
    # db.update_kline_1d_qfq()

    # db.update_stock_trades()
    db.update_stock_trades2()

    # r1 = db.con.sql(
    #     rf"SELECT * from read_parquet('{str(db.path_stock_trades / '20230504.parquet')}')")  # order by jj_code, datetime
    #
    # r4 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')")
    # r5 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'calender.parquet')}')")
    print(1)
