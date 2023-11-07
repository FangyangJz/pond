from typing import Tuple

import numpy as np
import pandas as pd
import requests


def bond_cb_redeem_jsl() -> pd.DataFrame:
    """
    集思录可转债-强赎
    https://www.jisilu.cn/data/cbnew/#redeem
    :return: 集思录可转债-强赎
    :rtype: pandas.DataFrame
    """
    url = "https://www.jisilu.cn/data/cbnew/redeem_list/"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "5",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.jisilu.cn",
        "Origin": "https://www.jisilu.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.jisilu.cn/data/cbnew/",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    params = {
        "___jsl": "LST___t=1653394005966",
    }
    payload = {
        "rp": "50",
    }
    r = requests.post(url, params=params, json=payload, headers=headers)
    data_json = r.json()
    temp_df = pd.DataFrame([item["cell"] for item in data_json["rows"]])
    temp_df.rename(columns={
        'bond_id': "代码",
        "bond_nm": "名称",
        'price': "现价",
        'stock_id': "正股代码",
        'stock_nm': "正股名称",
        'orig_iss_amt': "规模",
        'curr_iss_amt': "剩余规模",
        'convert_dt': "转股起始日",
        'convert_price': "转股价",
        'redeem_price_ratio': "强赎触发比",
        'real_force_redeem_price': "强赎价",
        'redeem_tc': "强赎条款",
        'sprice': "正股价",
        'redeem_icon': "强赎状态",
        'redeem_count': "强赎天计数",
        'force_redeem_price': "强赎触发价",
    }, inplace=True)

    temp_df = temp_df[
        [
            "代码",
            "名称",
            "现价",
            "正股代码",
            "正股名称",
            "规模",
            "剩余规模",
            "转股起始日",
            "转股价",
            "强赎触发比",
            "强赎触发价",
            "正股价",
            "强赎价",
            "强赎天计数",
            "强赎条款",
            "强赎状态",
        ]
    ]
    temp_df["现价"] = pd.to_numeric(temp_df["现价"])
    temp_df["规模"] = pd.to_numeric(temp_df["规模"])
    temp_df["剩余规模"] = pd.to_numeric(temp_df["剩余规模"])
    temp_df["转股起始日"] = pd.to_datetime(temp_df["转股起始日"]).dt.date
    temp_df["转股价"] = pd.to_numeric(temp_df["转股价"])
    temp_df["强赎触发比"] = pd.to_numeric(temp_df["强赎触发比"].str.strip("%"))
    temp_df["强赎触发价"] = pd.to_numeric(temp_df["强赎触发价"])
    temp_df["正股价"] = pd.to_numeric(temp_df["正股价"])
    temp_df["强赎价"] = pd.to_numeric(temp_df["强赎价"], errors="coerce")
    temp_df["强赎天计数"] = temp_df["强赎天计数"].replace(
        r"^.*?(\d{1,2}\/\d{1,2} \| \d{1,2}).*?$", r"\1", regex=True
    )
    temp_df["强赎状态"] = temp_df["强赎状态"].map(
        {"R": "已公告强赎", "O": "公告要强赎", "G": "公告不强赎", "B": "已满足强赎条件", "": ""}
    )
    return temp_df


def get_bond_cb_redeem_jsl_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    bond_cb_redeem_jsl_df = bond_cb_redeem_jsl().drop(
        labels=['正股价', '转股价'],
        axis=1
    )
    bond_cb_redeem_jsl_df['强赎天计数'] = bond_cb_redeem_jsl_df['强赎天计数'].replace(
        to_replace=r'\<.*\>',
        value="",
        regex=True
    )
    bond_cb_redeem_jsl_df['不安全天数'] = (
        bond_cb_redeem_jsl_df['强赎天计数']
        .apply(lambda x: int(x.split('/')[0]) if '/' in x else 30)
    )

    bond_cb_redeem_jsl_df['强赎状态'] = bond_cb_redeem_jsl_df['强赎状态'].replace(
        to_replace=r'^\s*$',
        value="不强赎",
        regex=True
    )
    bond_cb_redeem_jsl_df.rename(columns={"代码": "债券代码"}, inplace=True)
    redeem_jsl_df = bond_cb_redeem_jsl_df[~bond_cb_redeem_jsl_df['强赎状态'].str.contains('不强赎')].copy()
    return bond_cb_redeem_jsl_df, redeem_jsl_df


if __name__ == '__main__':
    df, r_df = get_bond_cb_redeem_jsl_df()
    print(1)
