# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 22:43
# @Author   : Fangyang
# @Software : PyCharm

import threading
import time
import numpy as np
import pandas as pd

from typing import Dict
from datetime import datetime
from tqdm import tqdm
from loguru import logger

from pond.akshare.const import (
    AKSHARE_CRAWL_CONCURRENT_LIMIT,
    AKSHARE_CRAWL_STOP_INTERVEL,
)
from pond.akshare.bond.cov_value_analysis import bond_zh_cov_value_analysis

import akshare as ak

from pond.akshare.stock.migrate_func import stock_zh_a_hist


def get_bond_daily_df(
    bond_code: str,
    bond_name: str,
    bond_scale: float,
    listing_date: datetime,
    stock_code: str,
    stock_name: str,
    market: str,
    res_dict: Dict[str, pd.DataFrame],
):
    """
    scale = row['发行规模'] if np.isnan(row['剩余规模']) else row['剩余规模']
    :param bond_scale:
    :param res_dict:
    :param bond_code:
    :param bond_name:
    :param listing_date:
    :param stock_code:
    :param stock_name:
    :param market:
    :return:
    """
    # print(f"start deal {bond_code} {bond_name}")

    if listing_date >= pd.Timestamp(datetime.now().date()):
        return

    market_str = "sh" if market == "SHSE" else "sz"
    try:
        # 2. 获取单只债券 ohlcv 全部历史信息
        bond_zh_hs_cov_daily_df = ak.bond_zh_hs_cov_daily(
            symbol=f"{market_str}{bond_code}"
        )
    except Exception as e:
        logger.warning(
            f"[bond_zh_hs_cov_daily] {market_str}{bond_code} {bond_name},  retry change market. {e}"
        )
        market_str = "sz" if market == "SHSE" else "sh"
        try:
            bond_zh_hs_cov_daily_df = ak.bond_zh_hs_cov_daily(
                symbol=f"{market_str}{bond_code}"
            )
            logger.success(
                f"[bond_zh_hs_cov_daily] {market_str}{bond_code} {bond_name},  retry change market pass"
            )
        except Exception as e1:
            logger.error(
                f"[bond_zh_hs_cov_daily] {market_str}{bond_code} {bond_name},  retry change market fail. {e1}"
            )
            return

    bond_zh_hs_cov_daily_df["bond_code"] = bond_code
    bond_zh_hs_cov_daily_df["bond_name"] = bond_name
    bond_zh_hs_cov_daily_df["stock_code"] = stock_code
    bond_zh_hs_cov_daily_df["stock_name"] = stock_name
    bond_zh_hs_cov_daily_df["bond_scale"] = bond_scale
    bond_zh_hs_cov_daily_df["listing_date"] = listing_date
    bond_zh_hs_cov_daily_df["market"] = market
    # 不加了, 因为存在下修问题, 需要另做处理, 后续因为拿到溢价率了, 所以这里忽略
    # bond_zh_hs_cov_daily_df['trans_stock_price'] = row['转股价']

    bond_zh_hs_cov_daily_df.rename(columns={"date": "trade_date"}, inplace=True)

    try:
        # 3. 获取单只债券历史 价值 和 溢价率
        bond_hist_df = (
            bond_zh_cov_value_analysis(symbol=bond_code)
            .rename(
                columns={
                    "日期": "trade_date",
                    "纯债价值": "bond_value",
                    "转股价值": "trans_stock_value",
                    "纯债溢价率": "bond_premium",
                    "转股溢价率": "trans_stock_premium",
                }
            )
            .drop(["收盘价", "bond_value", "bond_premium"], axis=1)
        )
    except Exception as e:
        logger.error(f"[bond_zh_cov_value_analysis] {bond_code} {bond_name}. {e}")
        return

        # 合并历史 ohclv 与 溢价率 表
    bond_df = bond_zh_hs_cov_daily_df.merge(bond_hist_df, on="trade_date")

    if bond_df.empty:
        return

    bond_df["amount"] = (
        (bond_df["close"] + bond_df["open"] + bond_df["high"] + bond_df["low"])
        * bond_df["volume"]
        / 4
    )
    bond_df["trade_date"] = pd.to_datetime(bond_df["trade_date"])
    bond_df["jj_code"] = bond_df["market"] + "." + bond_df["bond_code"]
    bond_df["duallow"] = bond_df["close"] + bond_df["trans_stock_premium"]

    nfq_df_columns_dict = {
        # trade_date2 只为调试过程中检查数据, 写入数据库时被过滤掉了
        "日期": "trade_date2",
        "开盘": "stock_open",
        "收盘": "stock_close",
        "最高": "stock_high",
        "最低": "stock_low",
        "成交量": "stock_volume",
        "成交额": "stock_amount",
        # '涨跌幅': "pct_chg",
        # 下面这俩字段在写数据库时被过滤掉了
        # '涨跌额': "pct_amount", '换手率': "turnover",
        # '振幅': 'amp'
    }
    start_date, end_date = bond_df["trade_date"].dt.strftime("%Y%m%d").values[[0, -1]]
    try:
        nfq_df = stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        )
        nfq_df = nfq_df[nfq_df_columns_dict.keys()].rename(columns=nfq_df_columns_dict)
    except Exception as e:
        logger.error(f"[stock_zh_a_hist] {stock_code} {stock_name} may be limited. {e}")
        return

    bond_df = pd.concat([bond_df, nfq_df], axis=1)

    res_dict[bond_code] = bond_df
    return bond_df


def update_bond_daily_res_dict_thread(
    bond_basic_df: pd.DataFrame, res_dict: Dict[str, pd.DataFrame]
):
    start_time = time.perf_counter()
    t_list = []
    for idx, row in tqdm(bond_basic_df.iterrows()):
        bond_code = row["债券代码"]
        bond_name = row["债券简称"]
        listing_date = pd.to_datetime(row["上市时间"])
        bond_scale = row["发行规模"] if np.isnan(row["剩余规模"]) else row["剩余规模"]
        stock_code = row["正股代码"]
        stock_name = row["正股简称"]
        market = row["market"]

        # For debug
        # df = get_bond_daily_df(
        #     bond_code, bond_name, bond_scale, listing_date,
        #     stock_code, stock_name, market, res_dict
        # )

        t = threading.Thread(
            target=get_bond_daily_df,
            args=(
                bond_code,
                bond_name,
                bond_scale,
                listing_date,
                stock_code,
                stock_name,
                market,
                res_dict,
            ),
        )
        t.start()
        t_list.append(t)

        if len(t_list) > AKSHARE_CRAWL_CONCURRENT_LIMIT:
            [t.join() for t in t_list]
            t_list = []
            time.sleep(AKSHARE_CRAWL_STOP_INTERVEL)

    if t_list:
        [t.join() for t in t_list]
    logger.success(
        f"Items: {len(res_dict)}/{len(bond_basic_df)}. "
        f"Threading time cost: {time.perf_counter() - start_time:.2f}s"
    )


def get_bond_index_daily():
    """
    名称	                类型	    描述
    price_dt            object	日期
    price               float64	指数
    amount              float64	剩余规模(亿元)
    volume              float64	成交额(亿元)
    count               int64	数量
    increase_val	    float64	涨跌
    increase_rt	        float64	涨幅
    avg_price	        float64	平均价格(元)
    mid_price	        float64	中位数价格(元)
    mid_convert_value	float64	中位数转股价值
    avg_dblow           float64	平均双底
    avg_premium_rt	    float64	平均溢价率
    mid_premium_rt	    float64	中位数溢价率
    avg_ytm_rt	        float64	平均收益率
    turnover_rt	        float64	换手率
    price_90	        int64	>90
    price_90_100	    int64	90~100
    price_100_110	    int64	100~110
    price_110_120	    int64	110~120
    price_120_130	    int64	120~130
    price_130	        int64	>130
    increase_rt_90	    float64	>90涨幅
    increase_rt_90_100	float64	90~100涨幅
    increase_rt_100_110	float64	100~110涨幅
    increase_rt_110_120	float64	110~120涨幅
    increase_rt_120_130	float64	120~130涨幅
    increase_rt_130	    float64	>130涨幅
    idx_price	        float64	沪深300指数
    idx_increase_rt	    float64	沪深300指数涨幅
    :return:
    """
    bond_cb_index_jsl_df = ak.bond_cb_index_jsl().rename(
        columns={"price_dt": "trade_date"}
    )
    return bond_cb_index_jsl_df


if __name__ == "__main__":
    from gulf.akshare.bond import get_bond_basic_df

    df1, df2 = get_bond_basic_df(data_delist_status="exclude")
    res_dict = dict()
    update_bond_daily_res_dict_thread(
        bond_basic_df=df1, res_dict=res_dict  # .iloc[:70],
    )
    print(1)

    df = get_bond_index_daily()
