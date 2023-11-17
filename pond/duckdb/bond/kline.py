# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/8 23:21
# @Author   : Fangyang
# @Software : PyCharm


import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm


def get_bond_daily_df_by_reader(
    bond_basic_df: pd.DataFrame, offset: int = 0
) -> pd.DataFrame:
    """
    优点: 读取速度快, 交易中的债几乎不存在缺失问题
    缺点:
        1. 退市债没有数据, 转股溢价率是本地计算, 如果存在复权问题, 可能会存在问题, 需要后续确认,
        2. 没有纯债价值和纯债溢价率

    :param bond_basic_df:
    :param offset: 大于等于0表示将全部数据写入db, -2 表示数据最近2天数据写入db
    """
    from mootdx.reader import Reader

    reader = Reader.factory(market="std", tdxdir="C:/new_tdx")

    res_dict = dict()
    for idx, row in (pbar := tqdm(bond_basic_df.iterrows())):
        bond_code = row["债券代码"]
        bond_name = row["债券简称"]
        pbar.set_postfix_str(f"{bond_code} {bond_name}")
        # price/10, volume*1000, amount keep
        price_cols = ["open", "high", "low", "close"]
        bond_daily_df = reader.daily(symbol=bond_code)
        bond_daily_df = bond_daily_df if offset >= 0 else bond_daily_df.iloc[offset:]

        if bond_daily_df.empty:
            continue
        bond_daily_df.loc[:, price_cols] = bond_daily_df[price_cols] / 10
        bond_daily_df.loc[:, "volume"] = bond_daily_df["volume"] * 1000

        stock_code = row["正股代码"]
        trans_stock_price = row["转股价"]

        # amount = price * volume * 100
        stock_daily_df = reader.daily(symbol=stock_code)
        stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
        stock_daily_df.columns = [f"stock_{c}" for c in stock_daily_df.columns]

        # 转股溢价率=可转债价格/转股价值-1，转股价值=可转债面值/转股价*正股价。
        dd = pd.concat([bond_daily_df, stock_daily_df], axis=1).dropna()
        dd["trans_stock_value"] = (
            100 / trans_stock_price * stock_daily_df["stock_close"]
        )  # 转股价值
        dd["trans_stock_premium"] = dd["close"] / dd["trans_stock_value"] - 1  # 转股溢价率
        # dd['bond_value'] =  # 纯债价值
        # dd['bond_premium'] =  # 纯债溢价率
        dd["duallow"] = dd["close"] + dd["trans_stock_premium"]
        dd["jj_code"] = row["jj_code"]
        dd["bond_scale"] = row["发行规模"] if np.isnan(row["剩余规模"]) else row["剩余规模"]
        dd["listing_date"] = pd.to_datetime(row["上市时间"])
        dd["stock_name"] = row["正股简称"]
        dd["market"] = row["market"]
        dd["bond_name"] = bond_name
        dd["bond_code"] = bond_code
        dd["stock_code"] = stock_code

        res_dict[bond_code] = dd

    start_datetime, end_datetime = dd.index[0], dd.index[-1]
    logger.success(
        f"Data from [{start_datetime}] to [{end_datetime}], offset:{offset} trade days"
    )
    df = (
        pd.concat(res_dict.values())
        .reset_index()
        .rename(columns={"date": "trade_date"})
    )
    res_dict.clear()

    return df


if __name__ == "__main__":
    from pond.duckdb.bond import BondDB
    from pathlib import Path

    db = BondDB(Path(r"D:\DuckDB"))
    dd = get_bond_daily_df_by_reader(db.bond_basic_df)
    print(1)
