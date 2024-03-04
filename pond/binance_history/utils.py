# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:30
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zipfile import ZipFile

import httpx
import polars as pl
import pandas as pd
from datetime import datetime

from pond.binance_history.exceptions import NetworkError
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType, Freq


def gen_data_url(
    data_type: DataType,
    asset_type: AssetType,
    freq: Freq,
    symbol: str,
    dt: datetime,
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


def get_urls(
    data_type: DataType,
    asset_type: AssetType,
    symbol: str,
    start: datetime,
    end: datetime,
    timeframe: TIMEFRAMES,
    file_path: Path,
    proxies: dict[str, str] = {},
) -> tuple[list[str], list[str]]:
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

        while (not get_local_data_path(last_month_url, file_path).exists()) and (
            not exists_month(last_month_url, proxies)
        ):
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
    load_months_urls = [
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
    download_months_urls = [
        url
        for url in load_months_urls
        if not get_local_data_path(url, file_path).exists()
    ]

    load_days_urls = [
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
    download_days_urls = [
        url
        for url in load_days_urls
        if not get_local_data_path(url, file_path).exists()
    ]

    load_urls = load_months_urls + load_days_urls
    download_urls = download_months_urls + download_days_urls
    return load_urls, download_urls


def get_local_data_path(url: str, local_path: Path | None = None) -> Path:
    path = urlparse(url).path

    if local_path:
        return local_path / path[1:]
    else:
        from pond.binance_history import config

        return config.CACHE_DIR / path[1:]


def load_data_from_disk(
    url: str,
    local_path: Path | None = None,
    dtypes: dict[str, Any] | None = None,
) -> pl.DataFrame | None:
    """
    binance data start 2022-01.zip has csv header, need adjust to this in case of lack of data
    for example lack of 2022-01-01 daily bar
    """
    path = get_local_data_path(url, local_path)

    if path.exists():
        if int(path.stem.split("-")[2]) < 2022:
            df = (
                pl.read_csv(
                    ZipFile(path).read(f"{path.stem}.csv"),
                    dtypes=dtypes,
                    # columns=list(dtypes.keys()) if dtypes else None,
                    has_header=False,
                )
                # .with_columns(
                #     (pl.col("open_time") * 1e3).cast(pl.Datetime),
                #     (pl.col("close_time") * 1e3).cast(pl.Datetime),
                # )
                # .to_pandas()
            )
        else:
            df = (
                pl.read_csv(
                    ZipFile(path).read(f"{path.stem}.csv"),
                    dtypes=dtypes,
                    columns=list(dtypes.keys()) if dtypes else None,
                    has_header=True,
                )
                # .with_columns(
                #     (pl.col("open_time") * 1e3).cast(pl.Datetime),
                #     (pl.col("close_time") * 1e3).cast(pl.Datetime),
                # )
                # .to_pandas()
            )

        return df

    else:
        return None

if __name__ == "__main__":
    import pandas as pd
    from dateutil import parser, tz

    months = pd.date_range(
        datetime(2023, 5, 1),
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
    month_urls, day_urls = get_urls(
        data_type=DataType.klines,
        asset_type=AssetType.future_um,
        symbol="BTCUSDT",
        start=start,
        end=end,
        timeframe="1m",
        file_path=Path("."),
    )
    print(1)
