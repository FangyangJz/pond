# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 22:37
# @Author   : Fangyang
# @Software : PyCharm
from functools import cache
import akshare as ak
import pandas as pd

from pond.akshare.bond.redeem import get_bond_cb_redeem_jsl_df
from pond.utils.code2code import (
    MARKET_SH,
    MARKET_SZ,
    JUEJIN_MARKET_MAP,
    trans_to_juejin_code,
)


@cache
def get_bond_basic_df(
    data_delist_status: str = "include",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回 (bond_zh_cov_df, redeem_df）

    data_delist_status: "include" / "exclude" , 包含退市债 / 不包含退市债

    全部转债信息(包含退市的)
    Index(['债券代码', '债券简称', '申购日期', '申购代码', '申购上限', '正股代码', '正股简称', '正股价', '转股价',
            '转股价值', '债现价', '转股溢价率', '原股东配售-股权登记日', '原股东配售-每股配售额', '发行规模', '中签号发布日',
            '中签率', '上市时间'],
            dtype='object')

    """

    # 1. 获取全部债券信息, 包含退市债券
    bond_zh_cov_df = ak.bond_zh_cov()

    bond_zh_cov_df.loc[
        bond_zh_cov_df["债券代码"].str.contains("^12\d{4}"), "market"
    ] = JUEJIN_MARKET_MAP[MARKET_SZ]
    bond_zh_cov_df.loc[
        bond_zh_cov_df["债券代码"].str.contains("^11\d{4}"), "market"
    ] = JUEJIN_MARKET_MAP[MARKET_SH]

    bond_cb_redeem_jsl_df, redeem_df = get_bond_cb_redeem_jsl_df()

    if data_delist_status == "include":
        bond_zh_cov_df = bond_zh_cov_df.merge(
            bond_cb_redeem_jsl_df, on=["债券代码", "正股代码"], how="outer"
        )
    elif data_delist_status == "exclude":
        bond_zh_cov_df = bond_zh_cov_df.merge(
            bond_cb_redeem_jsl_df, on=["债券代码", "正股代码"]
        )
        bond_zh_cov_df = bond_zh_cov_df[bond_zh_cov_df["强赎状态"].str.contains("不强赎")]

    bond_zh_cov_df.drop(
        labels=[
            "名称",
            "正股名称",
            "申购日期",
            "申购代码",
            "申购上限",
            "原股东配售-股权登记日",
            "原股东配售-每股配售额",
            "中签号发布日",
        ],
        axis=1,
        inplace=True,
    )

    # 去除 bond_cb_redeem_jsl_df 中的 EB 可交债
    bond_zh_cov_df = bond_zh_cov_df[~bond_zh_cov_df["债券简称"].isnull()]
    # 去除未上市债券
    bond_zh_cov_df = bond_zh_cov_df[~bond_zh_cov_df["上市时间"].isnull()]

    bond_zh_cov_df.reset_index(drop=True, inplace=True)

    bond_zh_cov_df["jj_code"] = bond_zh_cov_df["market"] + "." + bond_zh_cov_df["债券代码"]
    redeem_df["jj_code"] = redeem_df["债券代码"].apply(trans_to_juejin_code)

    return bond_zh_cov_df, redeem_df


if __name__ == "__main__":
    import time

    start_time = time.perf_counter()
    df1, df2 = get_bond_basic_df()
    print(f"Time cost: {time.perf_counter() - start_time}s")

    start_time = time.perf_counter()
    df1, df2 = get_bond_basic_df()
    print(f"Time cost: {time.perf_counter() - start_time}s")

    start_time = time.perf_counter()
    df1, df2 = get_bond_basic_df()
    print(f"Time cost: {time.perf_counter() - start_time}s")

    print(1)
