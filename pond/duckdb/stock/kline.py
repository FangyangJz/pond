# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/8 20:11
# @Author   : Fangyang
# @Software : PyCharm

import os
import time
from multiprocessing import Pool, Manager
from multiprocessing.managers import ListProxy

import pandas as pd
from loguru import logger
from tqdm import tqdm


def get_kline_1d_nfq_df(stock_basic_df: pd.DataFrame, offset: int=0) -> pd.DataFrame:
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
    logger.success(f'Concat all stocks history dataframe cost: {time.perf_counter() - concat_start_time:.2f}s')

    return df


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
    from pond.duckdb.stock import StockDB
    from pathlib import Path

    db = StockDB(Path(r'D:\DuckDB'))
    stock_basic_df = (
        db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')").df())

    df = get_kline_1d_nfq_df(stock_basic_df)
    print(1)