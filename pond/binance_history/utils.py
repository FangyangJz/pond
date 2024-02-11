# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:30
# @Author   : Fangyang
# @Software : PyCharm

import datetime
import io
import os
import os.path
import zipfile
from pathlib import Path
from typing import Optional, Union, Dict, Any
from urllib.parse import urlparse
from zipfile import ZipFile

import httpx
import polars as pl
import pandas as pd
import pendulum
from pandas import Timestamp, DataFrame

from pond.binance_history.exceptions import NetworkError, DataNotFound
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType, TIMEZONE, Freq


def gen_data_url(
    data_type: DataType,
    asset_type: AssetType,
    freq: Freq,
    symbol: str,
    dt: Timestamp,
    timeframe: TIMEFRAMES,
):
    if freq == Freq.monthly:
        date_str = dt.strftime("%Y-%m")
    elif freq == Freq.daily:
        date_str = dt.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"freq must be 'monthly' or 'daily', but got '{freq}'")

    if data_type == DataType.klines:
        url = (
            f"https://data.binance.vision/data/{asset_type.value}/{freq.value}/{data_type.value}/{symbol}/{timeframe}"
            f"/{symbol}-{timeframe}-{date_str}.zip"
        )
    elif data_type == DataType.aggTrades:
        url = (
            f"https://data.binance.vision/data/{asset_type.value}/{freq.value}/{data_type.value}/{symbol}"
            f"/{symbol}-{data_type}-{date_str}.zip"
        )
    else:
        raise ValueError(f"data_type must be 'klines', but got '{data_type}'")
    return url


def unify_datetime(input: Union[str, datetime.datetime]) -> pendulum.DateTime:
    if isinstance(input, str):
        return pendulum.parser.parse(input, strict=False).replace(tzinfo=None)  # type: ignore
    elif isinstance(input, datetime.datetime):
        return input.replace(tzinfo=None)
    else:
        raise TypeError(input)


def exists_month(month_url, proxies):
    try:
        resp = httpx.head(month_url, proxies=proxies, timeout=None)
    except (httpx.TimeoutException, httpx.NetworkError) as e:
        print(month_url)
        raise NetworkError(e)

    if resp.status_code == 200:
        return True
    elif resp.status_code == 404:
        return False
    else:
        raise NetworkError(resp.status_code)


def gen_download_urls(
    data_type: DataType,
    asset_type: AssetType,
    symbol: str,
    start: Timestamp,
    end: Timestamp,
    timeframe: TIMEFRAMES,
    proxies: Dict[str, str] = {},
):
    # assert start.tz is None and end.tz is None
    assert start <= end, "start cannot be greater than end"

    days = []
    months = pd.date_range(
        start.replace(day=1),
        end,
        freq="MS",
    ).to_list()
    if months:
        last_month_url = gen_data_url(
            data_type, asset_type, Freq.monthly, symbol, months[-1], timeframe=timeframe
        )
        while not exists_month(last_month_url, proxies):
            daily_month = months.pop()
            days = pd.date_range(
                daily_month,
                end,
                freq="D",
            ).to_list()

            if months:
                last_month_url = gen_data_url(
                    data_type,
                    asset_type,
                    Freq.monthly,
                    symbol,
                    months[-1],
                    timeframe=timeframe,
                )
            else:
                break

    months_urls = [
        gen_data_url(
            data_type=data_type,
            asset_type=asset_type,
            freq=Freq.monthly,
            symbol=symbol,
            dt=m,
            timeframe=timeframe,
        )
        for m in months
    ]

    days_urls = [
        gen_data_url(
            data_type=data_type,
            asset_type=asset_type,
            freq=Freq.daily,
            symbol=symbol,
            dt=m,
            timeframe=timeframe,
        )
        for m in days
    ]
    return months_urls, days_urls


def get_data(
    data_type: DataType,
    asset_type: AssetType,
    freq: str,
    symbol: str,
    dt: Timestamp,
    data_tz: TIMEZONE,
    timeframe: TIMEFRAMES,
    local_path: Union[Path, None] = None,
) -> Union[DataFrame, None]:
    if data_type == "klines":
        assert timeframe is not None

    url = gen_data_url(data_type, asset_type, freq, symbol, dt, timeframe)

    df = load_data_from_disk(url, local_path)
    if df is None:
        df = download_data(data_type, asset_type, data_tz, url)
        if df is not None:
            save_data_to_disk(url, df, local_path)
    return df


def download_data(
    data_type: DataType, asset_type: AssetType, data_tz: TIMEZONE, url: str
) -> Union[DataFrame, None]:
    assert isinstance(data_type, DataType)

    try:
        print(url)
        resp = httpx.get(url, timeout=None)
    except (httpx.TimeoutException, httpx.NetworkError) as e:
        raise NetworkError(e)

    if resp.status_code == 200:
        pass
    elif resp.status_code == 404:
        raise DataNotFound(url)
    else:
        raise NetworkError(url)

    if data_type == DataType.klines:
        return load_klines(asset_type, data_tz, resp.content)
    elif data_type == DataType.aggTrades:
        return load_agg_trades(data_tz, resp.content)


def load_klines(asset_type: AssetType, data_tz: TIMEZONE, content: bytes) -> DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as zipf:
        csv_name = zipf.namelist()[0]
        with zipf.open(csv_name, "r") as csvfile:
            skiprows = 0  # asset_type == 'spot'
            if asset_type in [AssetType.future_cm, AssetType.future_um]:
                skiprows = 1

            df = pd.read_csv(
                csvfile,
                usecols=range(9),
                header=None,
                skiprows=skiprows,
                names=[
                    "open_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_ms",
                    "quote_volume",
                    "trades",
                ],
            )
            df["open_datetime"] = pd.to_datetime(
                df.open_ms, unit="ms", utc=True
            ).dt.tz_convert(data_tz)
            df["close_datetime"] = pd.to_datetime(
                df.close_ms, unit="ms", utc=True
            ).dt.tz_convert(data_tz)
            del df["open_ms"]
            del df["close_ms"]
            df.set_index("open_datetime", inplace=True)
    return df


def load_agg_trades(data_tz: TIMEZONE, content: bytes) -> DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as zipf:
        csv_name = zipf.namelist()[0]
        with zipf.open(csv_name, "r") as csvfile:
            df = pd.read_csv(
                csvfile,
                header=0,
                usecols=[1, 2, 5, 6],
                names=["price", "quantity", "timestamp", "is_buyer_maker"],
            )
            df["datetime"] = pd.to_datetime(
                df.timestamp, unit="ms", utc=True
            ).dt.tz_convert(data_tz)
            del df["timestamp"]
            df.set_index("datetime", inplace=True)
    return df


def get_local_data_path(url: str, local_path: Union[Path, None] = None) -> Path:
    path = urlparse(url).path

    if local_path:
        return local_path / path[1:]
    else:
        from pond.binance_history import config

        return config.CACHE_DIR / path[1:]


def save_data_to_disk(
    url: str, df: DataFrame, local_path: Union[Path, None] = None
) -> None:
    path = get_local_data_path(url, local_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(path)


def load_data_from_disk(
    url: str,
    local_path: Union[Path, None] = None,
    dtypes: Union[Dict[str, Any], None] = None,
) -> Union[pl.DataFrame, None]:
    path = get_local_data_path(url, local_path)
    if path.exists():
        return pl.read_csv(ZipFile(path).read(f"{path.stem}.csv"), dtypes=dtypes)

    else:
        return None


if __name__ == "__main__":
    import pandas as pd
    from dateutil import parser, tz

    months = pd.date_range(
        Timestamp(2023, 5, 1),
        "2023-10-1",
        freq="MS",
    ).to_list()

    url_list = [
        gen_data_url(
            data_type=DataType.klines,
            asset_type=AssetType.future_um,
            freq=Freq.monthly,
            symbol="BTCUSDT",
            dt=m,
            timeframe="1m",
        )
        for m in months
    ]

    start = "2023-1-15"
    end = "2023-5-15"
    start = parser.parse(start).replace(tzinfo=tz.tzutc())
    end = parser.parse(end).replace(tzinfo=tz.tzutc())
    month_urls, day_urls = gen_download_urls(
        data_type=DataType.klines,
        asset_type=AssetType.future_um,
        symbol="BTCUSDT",
        start=start,
        end=end,
        timeframe="1m",
    )
    print(1)
