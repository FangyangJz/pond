# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm
# https://github.com/binance/binance-futures-connector-python

import time
import threading

import pandas as pd
import polars as pl
import datetime as dt
from tqdm import tqdm
from loguru import logger

from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures


def get_future_info_df(client: CMFutures | UMFutures) -> pd.DataFrame:
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


def get_future_symbol_list(client: CMFutures | UMFutures) -> list[str]:
    df = get_future_info_df(client)
    return [
        ss["symbol"] for _, ss in df.iterrows() if ss["contractType"] == "PERPETUAL"
    ]


def get_klines(client, symbol, interval, start, end, res_list):
    dd = client.klines(
        symbol=symbol, interval="1d", startTime=int(start_dt), endTime=int(end_dt)
    )
    dd = pl.from_records(dd, schema=kline_schema).with_columns(
        (pl.col("open_time") * 1e3).cast(pl.Datetime),
        (pl.col("close_time") * 1e3).cast(pl.Datetime),
        jj_code=pl.lit(symbol),
    )
    res_list.append(dd)


def update_res_dict_thread(
    client, symbols, interval, start: str, end: str, res_list: list[pl.DataFrame]
):
    start_time = time.perf_counter()
    t_list = []
    for symbol in tqdm(symbols, desc=""):
        t = threading.Thread(
            target=get_klines,
            args=(client, symbol, interval, start, end, res_list),
        )
        t.start()
        t_list.append(t)

    if t_list:
        [t.join() for t in t_list]

    logger.success(f"Threading cost: {time.perf_counter() - start_time:.2f}s")
    ddd = pl.concat(res_list).to_pandas()
    print(1)


if __name__ == "__main__":
    import polars as pl
    from pond.duckdb.crypto.const import kline_schema

    symbol = "BTSUSDT"
    interval = "1d"
    start = "2022-1-1"
    end = "2023-11-1"
    dt_format = "%Y-%m-%d"
    start_dt = dt.datetime.strptime(start, dt_format).timestamp() * 1e3
    end_dt = dt.datetime.strptime(end, dt_format).timestamp() * 1e3
    # proxies = {"https": "127.0.0.1:7890"}
    proxies = {}

    um_client = UMFutures(proxies=proxies)
    cm_client = CMFutures(proxies=proxies)

    res_list = []
    symbols = ["BTCUSDT", "BTSUSDT", "ETHUSDT"]
    update_res_dict_thread(um_client, symbols, interval, start_dt, end_dt, res_list)

    # r = get_future_info_df(um_client)
    dd = um_client.klines(
        symbol=symbol, interval=interval, startTime=int(start_dt), endTime=int(end_dt)
    )
    df = (
        pl.from_records(dd, schema=kline_schema)
        .with_columns(
            (pl.col("open_time") * 1e3).cast(pl.Datetime),
            (pl.col("close_time") * 1e3).cast(pl.Datetime),
            jj_code=pl.lit(symbol),
        )
        .to_pandas()
    )

    cm_symbol_list = get_future_symbol_list(cm_client)
    um_symbol_list = get_future_symbol_list(um_client)

    tz = "Asia/Shanghai"
    # r = client.continuous_klines("BTCUSD", "PERPETUAL", "1m")
    print(1)
