# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 21:58
# @Author   : Fangyang
# @Software : PyCharm

import threading
import time
import pandas as pd

from typing import Dict
from tqdm import tqdm
from loguru import logger

from pond.akshare.const import AKSHARE_CRAWL_CONCURRENT_LIMIT, AKSHARE_CRAWL_STOP_INTERVEL
from pond.akshare.stock.migrate_func import stock_zh_a_hist
from pond.utils.stock import get_stock_ch_market


def get_stock_hist_df(
        res_dict: Dict[str, pd.DataFrame],
        code: str = '000001', name: str = "平安银行",
        start_date: str = "19700101",
        end_date: str = "20500101",
) -> pd.DataFrame:
    """
    和 tushare pro 的数据相比:
    1. 多了 换手率, 但是和 tushare 每日 indicator 中有 turnover, merge的时候补上了
    2. name 不为 None 就多了 名称 ;
    3. 少了复权因子, 交易状态, 平均价格

    这里爬虫获取的 振幅 = (当日最高价 - 当日最低价) / 昨日收盘价 * 100%
    tushare pro 的 change 叫 涨跌 是 (今日close - 昨日close)

    :param code: 000001
    :param name:
    :param start_date:
    :param end_date:
    :param res_dict:
    :return:
    """
    hfq_df_columns_dict = {
        '日期': "trade_date",
        '开盘': "adj_open", '收盘': "adj_close", '最高': "adj_high", '最低': "adj_low",
        '成交量': "volume", '成交额': "amount",
        '涨跌幅': "pct_chg",
        # 下面这俩字段在写数据库时被过滤掉了
        '涨跌额': "pct_amount", '换手率': "turnover",
        '振幅': 'amp'
    }
    try:
        hfq_df = stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_date,
            adjust="hfq"
        )

        if hfq_df.empty:
            logger.error(f"[stock_zh_a_hist]{code} {name} hfq dataframe is empty. Maybe limited.")
            return pd.DataFrame()

        hfq_df = hfq_df[hfq_df_columns_dict.keys()].rename(columns=hfq_df_columns_dict)

        nfq_df_columns_dict = {'开盘': "open", '收盘': "close", '最高': "high", '最低': "low"}
        nfq_df = stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_date,
            adjust=""
        )[nfq_df_columns_dict.keys()].rename(
            columns=nfq_df_columns_dict
        )
    except Exception as e:
        logger.error(f'[stock_zh_a_hist]{code} {name}. {e}')
        return pd.DataFrame()

    df = pd.concat([hfq_df, nfq_df], axis=1)

    if name is not None:
        df['name'] = name

    res_dict[code] = df

    return df


def update_res_dict_thread(df: pd.DataFrame, start_date_str: str, end_date_str: str, res_dict: dict):
    start_time = time.perf_counter()
    t_list = []
    for idx, item in tqdm(df.iterrows(), desc="Get stock hist df from akshare"):
        code, name = item[0], item[1]

        ch_market = get_stock_ch_market(code)
        if ch_market not in ['沪A股主板', '深A股主板', '中小板', '创业板']:
            logger.info(f'{code} {name} 是 {ch_market} 股票, 跳过')
            continue

        t = threading.Thread(target=get_stock_hist_df, args=(res_dict, code, name, start_date_str, end_date_str))
        t.start()
        t_list.append(t)

        if len(t_list) > AKSHARE_CRAWL_CONCURRENT_LIMIT:
            [t.join() for t in t_list]
            t_list = []
            time.sleep(AKSHARE_CRAWL_STOP_INTERVEL)

    if t_list:
        [t.join() for t in t_list]

    logger.success(f"Items: {len(res_dict)}/{len(df)}. Threading cost: {time.perf_counter() - start_time:.2f}s")


if __name__ == '__main__':
    from gulf.akshare.stock.all_basic import get_all_stocks_df

    stock_zh_a_spot_em_df = get_all_stocks_df()
    res_dict = dict()

    # 单日数据下载12分钟, 全历史数据下载40多分钟
    update_res_dict_thread(
        df=stock_zh_a_spot_em_df[['代码', '名称']],
        start_date_str="20220902",
        end_date_str="20220902",
        res_dict=res_dict
    )

    res_list = []
    for k, v in tqdm(res_dict.items(), desc=f"Concating res_df"):
        # copy: 00:01:24
        # no copy : 00:02:35
        # list append then concat once: 00:00:00
        res_list.append(v)
    res_df = pd.concat(res_list)
    print(1)
