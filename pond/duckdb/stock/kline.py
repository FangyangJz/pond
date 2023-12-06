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

from pond.duckdb.type import DataFrameType
from pond.tdx.finance_cw import update_cw_data, get_cw_dict_acc
from pond.tdx.fq import qfq_acc
from pond.tdx.path import gbbq_path, tdx_path


def get_kline_1d_qfq_df(stock_basic_df: DataFrameType, offset: int = 1) -> pd.DataFrame:
    stock_basic_df = (
        stock_basic_df
        if isinstance(stock_basic_df, pd.DataFrame)
        else stock_basic_df.to_pandas()
    )

    # 更新本地财务数据
    update_cw_data()
    # 读取财务数据, 获取流通股本数据
    cw_dict = get_cw_dict_acc()
    # 股本变迁数据 tdx 客户端会自动更新
    df_gbbq = pd.read_csv(gbbq_path / "gbbq.csv", dtype={"code": str})
    res_dict1 = qfq_acc(stock_basic_df, df_gbbq, cw_dict, offset)

    concat_start_time = time.perf_counter()
    logger.info("Start to concat all stocks qfq history dataframe ...")
    df = pd.concat(res_dict1.values()).reset_index(drop=True)
    logger.success(
        f"Concat all stocks qfq history dataframe cost: {time.perf_counter() - concat_start_time:.2f}s"
    )

    return df


def get_kline_1d_nfq_df(stock_basic_df: DataFrameType, offset: int = 1) -> pd.DataFrame:
    stock_basic_df = (
        stock_basic_df
        if isinstance(stock_basic_df, pd.DataFrame)
        else stock_basic_df.to_pandas()
    )

    pool = Pool(os.cpu_count() - 1)
    step = int(len(stock_basic_df) / (4 * pool._processes))  # tune coe 4 get best speed
    _manager = Manager()
    res_list = _manager.list()

    pbar1 = tqdm(total=int(len(stock_basic_df) / step), position=0, leave=True)
    pbar1.set_description(
        f"Function update_kline_1d_nfq with multiprocess, total {len(stock_basic_df)}, step {step}"
    )

    for i in range(0, len(stock_basic_df), step):
        pool.apply_async(
            update_res_list,
            args=(stock_basic_df.iloc[i : i + step], offset, res_list),
            callback=lambda *args: pbar1.update(),
        )
    pool.close()
    pool.join()
    pbar1.close()

    concat_start_time = time.perf_counter()
    logger.info("Start to concat all stocks history dataframe ...")
    df = pd.concat(res_list).reset_index(names=["trade_date"])
    logger.success(
        f"Concat all stocks history dataframe cost: {time.perf_counter() - concat_start_time:.2f}s"
    )

    return df


def update_res_list(
    stock_basic_slice_df: pd.DataFrame, offset: int, global_res_list: ListProxy
):
    from mootdx.reader import Reader

    reader = Reader.factory(market="std", tdxdir=tdx_path)

    # TODO multiprocess accelerate
    # for idx, row in (pbar := tqdm(stock_basic_slice_df.iterrows())):
    for idx, row in stock_basic_slice_df.iterrows():
        stock_code = row["代码"]
        # pbar.set_postfix_str(f"{row['jj_code']} - {row['名称']} - {row['所处行业']}")

        stock_daily_df = reader.daily(symbol=stock_code)
        stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
        if stock_daily_df.empty:
            continue

        stock_daily_df["jj_code"] = row["jj_code"]
        stock_daily_df["name"] = row["名称"]
        stock_daily_df["industry"] = row["所处行业"]

        global_res_list.append(stock_daily_df)


if __name__ == "__main__":
    from pond.duckdb.stock import StockDB
    from pathlib import Path

    db = StockDB(Path(r"E:\DuckDB"))

    qfq_df = get_kline_1d_qfq_df(db.stock_basic_df)

    nfq_df = get_kline_1d_nfq_df(db.stock_basic_df)
    print(1)
