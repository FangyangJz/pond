# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:01
# @Author   : Fangyang
# @Software : PyCharm

import os
import time
from typing import List

import pandas as pd
from tqdm import tqdm
from pathlib import Path
from multiprocessing import Pool, Manager
from loguru import logger


def get_trade_df(csv_file: Path):
    cols_dict = {'万得代码': 'code', '自然日': 'date', '时间': 'time', 'BS标志': 'BS', '成交价格': 'price',
                 '成交数量': 'volume', '叫卖序号': 'id_sell', '叫买序号': 'id_buy'}
    keep_cols = ['jj_code', 'datetime', 'BS', 'price', 'volume', 'id_sell', 'id_buy']

    df = pd.read_csv(
        csv_file, encoding='gbk',
        dtype={
            '万得代码': str,
            '交易所代码': int,
            '自然日': int,
            '时间': int,
            '成交编号': int,
            '成交代码': str,
            '委托代码': str,
            'BS标志': str,
            '成交价格': float,
            '成交数量': int,
            '叫卖序号': int,
            '叫买序号': int
        }
    ).dropna(axis=1).rename(columns=cols_dict)[list(cols_dict.values())]

    df['datetime'] = pd.to_datetime(
        df['date'].astype(str) + df['time'].astype(str).str.zfill(9), format='%Y%m%d%H%M%S%f')
    df['jj_code'] = df['code'].apply(lambda x: f"{x[-2:]}SE.{x[:6]}" if len(x) == 9 else f"{x[-2:]}SE.{x[:5]}")
    df['price'] = df['price'] / 1e4

    return df[keep_cols]


def update_res_list(file_path_list: List[Path], res_list):
    for file in file_path_list:
        res_list.append(get_trade_df(file))


def get_trade_df_with_multiprocess(dir_path: Path) -> pd.DataFrame:
    pool = Pool(os.cpu_count() - 1)

    # set progress bar
    files_list = list(dir_path.glob('*\逐笔成交.csv'))
    step = int(len(files_list) / (4 * pool._processes))  # tune coe 4 get best speed
    pbar = tqdm(total=int(len(files_list) / step))
    pbar.set_description(f'Function get_trade_df_with_multiprocess, total {len(files_list)}, step {step}')

    # set multiprocess
    _manager = Manager()
    res_list = _manager.list()
    for i in range(0, len(files_list), step):
        pool.apply_async(
            update_res_list,
            args=(files_list[i:i + step], res_list),
            callback=lambda *args: pbar.update()
        )

    pool.close()
    pool.join()

    logger.success(f'Multiprocess result res_list length : {len(res_list)}')
    start_time = time.perf_counter()
    total_df = pd.concat(res_list)
    logger.success(f'Concat df cost: {time.perf_counter() - start_time}s')
    _manager.shutdown()

    return total_df


if __name__ == '__main__':
    import duckdb

    date_str = '20230505'
    dir_path = Path(rf'D:\A_Projects\python_projects\avalon\avalon\backtest\juejin\bond_end2end\{date_str}')

    df2 = get_trade_df_with_multiprocess(dir_path)

    con = duckdb.connect()
    compress = 'ZSTD'
    start_time = time.perf_counter()
    con.sql('select * from df2').write_parquet(f'trade_{date_str}.parquet', compression=compress)
    print(f'cost: {time.perf_counter() - start_time}s')
