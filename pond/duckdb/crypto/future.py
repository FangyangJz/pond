# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm

from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures
from typing import List, Union

import pandas as pd
import datetime as dt

# https://github.com/binance/binance-futures-connector-python


def get_future_info_df(client: Union[CMFutures, UMFutures]) -> pd.DataFrame:
    info = client.exchange_info()
    df = pd.DataFrame.from_records(info["symbols"])
    df["update_datetime"] = dt.datetime.utcfromtimestamp(
        info["serverTime"] / 1000
    ).replace(tzinfo=dt.timezone.utc)
    df["deliveryDate"] = df["deliveryDate"].apply(
        lambda x: dt.datetime.utcfromtimestamp(x / 1000).replace(tzinfo=dt.timezone.utc)
    )
    df["onboardDate"] = df["onboardDate"].apply(
        lambda x: dt.datetime.utcfromtimestamp(x / 1000).replace(tzinfo=dt.timezone.utc)
    )
    return df


def get_future_symbol_list(client: Union[CMFutures, UMFutures]) -> List[str]:
    df = get_future_info_df(client)
    return [
        ss["symbol"] for _, ss in df.iterrows() if ss["contractType"] == "PERPETUAL"
    ]


if __name__ == "__main__":
    proxies = {"https": "127.0.0.1:7890"}

    um_client = UMFutures(proxies=proxies)
    cm_client = CMFutures(proxies=proxies)

    # r = get_future_info_df(um_client)

    cm_symbol_list = get_future_symbol_list(cm_client)
    um_symbol_list = get_future_symbol_list(um_client)

    start = "2023-1-1"
    end = "2023-11-1"
    tz = "Asia/Shanghai"
    # r = client.continuous_klines("BTCUSD", "PERPETUAL", "1m")
    print(1)
