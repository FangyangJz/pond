# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm
import time

from binance.cm_futures import CMFutures
from typing import List

from tqdm import tqdm
import pandas as pd
import datetime as dt

# https://github.com/binance/binance-futures-connector-python


def get_cm_future_info_df(client: CMFutures)->pd.DataFrame:
    info = client.exchange_info()
    df = pd.DataFrame.from_records(info['symbols'])
    df['update_datetime'] = dt.datetime.utcfromtimestamp(info['serverTime']/1000).replace(tzinfo=dt.timezone.utc)
    df['deliveryDate'] = df['deliveryDate'].apply(lambda x : dt.datetime.utcfromtimestamp(x/1000).replace(tzinfo=dt.timezone.utc))
    df['onboardDate'] = df['onboardDate'].apply(lambda x : dt.datetime.utcfromtimestamp(x/1000).replace(tzinfo=dt.timezone.utc))
    return df

def get_cm_future_symbol_list(client: CMFutures) -> List[str]:
    df = get_cm_future_info_df(client)
    return [i["symbol"] for i in df["symbols"].values if i["contractType"] == "PERPETUAL"]


if __name__ == "__main__":
    from pond.binance_history.api import fetch_klines

    proxies = {"https": "127.0.0.1:7890"}

    client = CMFutures(
        proxies=proxies
    )
    cm_symbol_list = get_cm_future_symbol_list(client)
    asset_type = "futures/cm"

    start = "2023-1-1"
    end = "2023-11-1"
    tz = "Asia/Shanghai"

    for symbol in tqdm(cm_symbol_list):
        start_time = time.perf_counter()
        klines = fetch_klines(
            symbol=symbol, start=start, end=end, tz=tz, asset_type=asset_type
        )
        print(symbol, klines.shape, time.perf_counter()-start_time)
        print(1)
    # r = client.klines("BTCUSD_PERP", "1d")
    # info = client.exchange_info()
    # cm_list = []
    # for i in info["symbols"]:
    #     if i["contractType"] == "PERPETUAL":
    #         cm_list.append(i)
    #
    # r = client.continuous_klines("BTCUSD", "PERPETUAL", "1m")
    print(1)
