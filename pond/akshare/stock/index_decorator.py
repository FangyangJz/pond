# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2023/1/26 0:37
# @Author   : Fangyang
# @Software : PyCharm

import pandas as pd


def trans_ch_col_name(func):
    def inner(*args, **kwargs) -> pd.DataFrame:
        df: pd.DataFrame = func(*args, **kwargs)
        df.rename(columns={
            '板块名称': "jj_code",
            "日期": "trade_date",
            "日期时间": "trade_date",
            '开盘': "open", '收盘': "close", '最高': 'high', '最低': 'low',
            '涨跌幅': "pct_chg", '涨跌额': "pct_amount",
            '成交量': "vol", '成交额': "amount", '振幅': 'hl_chg', '换手率': "turnover_rate",
        }, inplace=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df

    return inner


if __name__ == '__main__':
    pass
