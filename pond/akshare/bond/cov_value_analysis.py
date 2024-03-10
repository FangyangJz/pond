# !/usr/bin/env python3
# @Datetime : 2022/12/15 23:37
# @Author   : Fangyang
# @Software : PyCharm

import pandas as pd

from pond.utils.crawler import request_session


def bond_zh_cov_value_analysis(symbol: str = "123138") -> pd.DataFrame:
    """
    https://data.eastmoney.com/kzz/detail/113527.html
    东方财富网-数据中心-新股数据-可转债数据-价值分析-溢价率分析
    :return: 可转债价值分析
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/get"
    params = {
        "sty": "ALL",
        "token": "894050c76af8597a853f5b408b759f5d",
        "st": "date",
        "sr": "1",
        "source": "WEB",
        "type": "RPTA_WEB_KZZ_LS",
        "filter": f'(zcode="{symbol}")',
        "p": "1",
        "ps": "8000",
        "_": "1648629088839",
    }

    ses = request_session()
    r = ses.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.columns = [
        "日期",
        "-",
        "-",
        "转股价值",
        "纯债价值",
        "纯债溢价率",
        "转股溢价率",
        "收盘价",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            "日期",
            "收盘价",
            "纯债价值",
            "转股价值",
            "纯债溢价率",
            "转股溢价率",
        ]
    ]

    temp_df["日期"] = pd.to_datetime(temp_df["日期"]).dt.date
    temp_df["收盘价"] = pd.to_numeric(temp_df["收盘价"])
    temp_df["纯债价值"] = pd.to_numeric(temp_df["纯债价值"])
    temp_df["转股价值"] = pd.to_numeric(temp_df["转股价值"])
    temp_df["纯债溢价率"] = pd.to_numeric(temp_df["纯债溢价率"])
    temp_df["转股溢价率"] = pd.to_numeric(temp_df["转股溢价率"])

    return temp_df


if __name__ == "__main__":
    pass
