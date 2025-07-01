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
from tenacity import retry, stop_after_delay, wait_fixed
from concurrent.futures import ThreadPoolExecutor


@retry(wait=wait_fixed(3), stop=stop_after_delay(10))
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


def mock_empty_klines(start: int, end: int, coef: int) -> pl.DataFrame:
    return (
        pl.DataFrame(
            {
                "open_time": pl.arange(start, end, coef * 1000, eager=True),
                "volume": 0.0,
                "quote_volume": 0.0,
                "taker_buy_volume": 0.0,
                "taker_buy_quote_volume": 0.0,
                "ignore": 0.0,
            },
        )
        .with_columns(
            pl.lit(0, pl.Int64).alias("count"),
            pl.lit(None, pl.Float64).alias("open"),
            pl.lit(None, pl.Float64).alias("high"),
            pl.lit(None, pl.Float64).alias("low"),
            pl.lit(None, pl.Float64).alias("close"),
            (pl.col("open_time") + coef * 1000 - 1).alias("close_time"),
        )
        .select(list(klines_schema.keys()))
        # .with_columns(
        #     (pl.col("open_time") * 1e3).cast(pl.Datetime),
        #     (pl.col("close_time") * 1e3).cast(pl.Datetime),
        # )
        # .sort("open_time")
    )


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
    coef = 1
    if interval == "1d":
        coef = 60 * 60 * 24
        limit = int(deltaTime / coef) + 1
    elif interval == "1h":
        coef = 60 * 60
        limit = int(deltaTime / coef) + 1
    elif interval == "15m":
        coef = 60 * 15
        limit = int(deltaTime / coef) + 1
    elif interval == "5m":
        coef = 60 * 5
        limit = int(deltaTime / coef) + 1
    else:
        raise ValueError(f"Invalid interval: {interval}")

    logger.info(f"Requesting limit {limit} klines for {symbol} {interval}")
    if (symbol in ["ICPUSDT"]) and (limit > 1500):
        dd = mock_empty_klines(start, end, coef)

    elif limit > 1500:
        logger.info(
            f"Requesting limit {limit} > 1500 for {symbol} {interval}, loop limit fix 499."
        )
        fix_limit = 499
        # raise ValueError("manual breakpoint")
        while True:
            dd = (
                pl.from_records(
                    client.klines(
                        symbol=symbol,
                        interval=interval,
                        # startTime=start,
                        endTime=end,
                        limit=fix_limit,
                    ),
                    schema=klines_schema,
                )
                # .with_columns(
                #     (pl.col("open_time") * 1e3).cast(pl.Datetime),
                #     (pl.col("close_time") * 1e3).cast(pl.Datetime),
                #     jj_code=pl.lit(symbol),
                # )
                # .sort("open_time")
            )
            limit -= fix_limit
            if limit < (1 - 499):
                break
            else:
                res_list.append(dd)
    else:
        dd = client.klines(
            symbol=symbol,
            interval=interval,
            startTime=start,
            endTime=end,
            limit=limit,
        )
        dd = (
            pl.from_records(dd, schema=klines_schema, strict=False)
            # .with_columns(
            #     (pl.col("open_time") * 1e3).cast(pl.Datetime),
            #     (pl.col("close_time") * 1e3).cast(pl.Datetime),
            #     jj_code=pl.lit(symbol),
            # )
            # .sort("open_time")
        )
        if dd.is_empty():
            logger.warning(
                f"No data returned for {symbol} {interval}, start:{start}, end:{end}, limit:{limit}. "
                f"Use mock empty DataFrame replace."
            )
            dd = mock_empty_klines(start, end, coef)
        else:
            logger.success(
                f"Retrieved {dd.shape[0]} klines for {symbol} {interval}, start:{start}, end:{end}, limit:{limit}"
            )
    res_list.append(dd[1:])


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
