# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm
import time

from binance.cm_futures import CMFutures
from typing import List

from tqdm import tqdm


# https://github.com/binance/binance-futures-connector-python


def get_cm_future_symbol_list(client: CMFutures) -> List[str]:
    info = client.exchange_info()
    return [i["symbol"] for i in info["symbols"] if i["contractType"] == "PERPETUAL"]


if __name__ == "__main__":
    from pond.binance_history.api import fetch_klines

    proxies = {"https": "127.0.0.1:7890"}

    client = CMFutures(
        # proxies=proxies
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
