# !/usr/bin/env python3
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm
# https://github.com/binance/binance-futures-connector-python

import pandas as pd
import polars as pl
import datetime as dt
from loguru import logger

from binance.spot import Spot
from binance.cm_futures import CMFutures
from binance.um_futures import UMFutures
from pond.duckdb.crypto.const import klines_schema
from pond.binance_history.type import TIMEFRAMES
from tenacity import retry, wait_fixed
from concurrent.futures import ThreadPoolExecutor


@retry(wait=wait_fixed(3))
def get_future_info_df(client: CMFutures | UMFutures | Spot) -> pd.DataFrame:
    info = client.exchange_info()
    df = pd.DataFrame.from_records(info["symbols"])
    df["update_datetime"] = dt.datetime.fromtimestamp(
        info["serverTime"] / 1000, dt.timezone.utc
    )
    if not isinstance(client, Spot):
        df["deliveryDate"] = df["deliveryDate"].apply(
            lambda x: dt.datetime.fromtimestamp(x / 1000, dt.timezone.utc)
        )
        df["onboardDate"] = df["onboardDate"].apply(
            lambda x: dt.datetime.fromtimestamp(x / 1000, tz=dt.timezone.utc)
        )
    return df


def get_future_symbol_list(client: CMFutures | UMFutures) -> list[str]:
    df = get_future_info_df(client)
    return [
        ss["symbol"] for _, ss in df.iterrows() if ss["contractType"] == "PERPETUAL"
    ]


@retry(wait=wait_fixed(3))
def get_klines(
    client: CMFutures | UMFutures | Spot,
    symbol: str,
    interval: TIMEFRAMES,
    start: int,
    end: int,
    res_list: list[pl.DataFrame],
):
    deltaTime = (end - start) / 1000
    if interval == "1d":
        limit = int(deltaTime / 60 / 60 / 24) + 1
    elif interval == "1h":
        limit = int(deltaTime / 60 / 60) + 1
    elif interval == "5m":
        limit = int(deltaTime / 60 / 5) + 1
    else:
        raise ValueError(f"Invalid interval: {interval}")

    logger.info(f"Requesting limit {limit} klines for {symbol} {interval}")
    dd = client.klines(
        symbol=symbol,
        interval=interval,
        startTime=start,
        endTime=end,
        limit=limit,
    )
    dd = pl.from_records(dd, schema=klines_schema)
    res_list.append(dd)


def get_supply_df(
    client: CMFutures | UMFutures | Spot,
    lack_df: pl.DataFrame,
    symbol: str,
    interval: TIMEFRAMES = "1d",
) -> pl.DataFrame:
    """
    base = 1577836800000
    base_dt = dt.datetime.utcfromtimestamp(base/1e3)
    base += dt.timedelta(days=1).total_seconds()*1e3
    base_dt2 = dt.datetime.utcfromtimestamp(base/1e3)
    """
    res_list = []
    with ThreadPoolExecutor(max_workers=20) as executor:  # 设置线程池的最大线程数为10
        futures = []
        for i in range(0, len(lack_df), 2):
            start = lack_df["open_time"][i]
            end = lack_df["open_time"][i + 1]
            logger.info(
                f"[{symbol}] Supplement missing {interval} data: "
                f"{dt.datetime.fromtimestamp(start / 1e3, dt.timezone.utc)} -> "
                f"{dt.datetime.fromtimestamp(end / 1e3, dt.timezone.utc)}"
            )
            # 使用线程池提交任务
            future = executor.submit(
                get_klines, client, symbol, interval, start, end, res_list
            )
            futures.append(future)

    # 等待所有线程任务完成
    for future in futures:
        future.result()

    return pl.concat(res_list)


if __name__ == "__main__":
    import polars as pl
    from pond.duckdb.crypto.const import klines_schema

    symbol = "IOTAUSDT"
    client = UMFutures(proxies={"http": "127.0.0.1:7890", "https": "127.0.0.1:7890"})
    dd = client.klines(
        symbol=symbol,
        interval="5m",
        startTime=1645833300000,
        endTime=1646092800000,
        limit=1500,
    )
    dd = (
        pl.from_records(dd, schema=klines_schema)
        .with_columns(
            (pl.col("open_time") * 1e3).cast(pl.Datetime),
            (pl.col("close_time") * 1e3).cast(pl.Datetime),
            jj_code=pl.lit(symbol),
        )
        .sort("open_time")
    )

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
    # r = get_future_info_df(um_client)
    dd = um_client.klines(
        symbol=symbol, interval=interval, startTime=int(start_dt), endTime=int(end_dt)
    )
    df = (
        pl.from_records(dd, schema=klines_schema)
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
