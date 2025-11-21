# !/usr/bin/env python3
# @Datetime : 2023/12/23 下午 09:30
# @Author   : Fangyang
# @Software : PyCharm

from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from zipfile import ZipFile
from loguru import logger
from datetime import datetime
import threading

import polars as pl
import pandas as pd
import httpx

from pond.utils.crawler import get_mock_headers
from pond.binance_history.vision import get_vision_data_url_list
from pond.binance_history.exceptions import NetworkError
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType, Freq
from pond.duckdb.crypto.const import timeframe_data_types_dict


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
    else:
        url = (
            f"https://data.binance.vision/data/{asset_type.value}/{freq.value}/{data_type.value}/{symbol}"
            f"/{symbol}-{data_type.value}-{date_str}.zip"
        )

    return url


def ping_url_is_exist(url: str, proxies: dict[str, str]) -> bool:
    try:
        logger.info(f"Ping {url}")
        resp = httpx.head(
            url, proxies=proxies, timeout=None, headers=get_mock_headers()
        )
    except (httpx.TimeoutException, httpx.NetworkError) as e:
        logger.error(url)
        raise NetworkError(e)

    if resp.status_code == 200:
        return True
    elif resp.status_code == 404:
        logger.warning(f"[404] {url}")
        return False
    else:
        raise NetworkError(resp.status_code)


def update_res_dict(
    last_month_url,
    timestamp: pd.Timestamp,
    file_path,
    proxies,
    local_dict: dict[str, pd.Timestamp],
    network_dict: dict[str, pd.Timestamp],
):
    if get_local_data_path(last_month_url, file_path).exists():
        local_dict[last_month_url] = timestamp
    elif ping_url_is_exist(last_month_url, proxies):
        network_dict[last_month_url] = timestamp


def get_urls_by_threading_ping(
    data_type: DataType,
    asset_type: AssetType,
    symbol: str,
    start: datetime,
    end: datetime,
    timeframe: TIMEFRAMES,
    file_path: Path,
    proxies: dict[str, str] = {},
) -> tuple[list[str], list[str]]:
    assert start <= end, "start cannot be greater than end"

    months = pd.date_range(
        start.replace(day=1),
        end,
        freq="MS",
    ).to_list()

    local_month_dict = {}
    network_month_dict = {}
    t_list = []
    for m in months:
        last_month_url = gen_data_url(
            data_type, asset_type, Freq.monthly, symbol, m, timeframe=timeframe
        )
        t = threading.Thread(
            target=update_res_dict,
            args=(
                last_month_url,
                m,
                file_path,
                proxies,
                local_month_dict,
                network_month_dict,
            ),
        )
        t.start()
        t_list.append(t)

    if t_list:
        [t.join() for t in t_list]

    all_dict = network_month_dict | local_month_dict
    if len(all_dict) == 0:
        return [], []

    all_keys = list(all_dict.keys())
    all_keys.sort()

    local_day_dict = {}
    network_day_dict = {}
    t_list = []
    start = all_dict[all_keys[-1]] + pd.DateOffset(months=1)
    # 3*31 表示 3个月没有月数据, 用3个月的日数据补充
    days_range_list = pd.date_range(start, end, freq="D").to_list()[: 3 * 31]
    for d in days_range_list:
        day_url = gen_data_url(
            data_type, asset_type, Freq.daily, symbol, d, timeframe=timeframe
        )
        t = threading.Thread(
            target=update_res_dict,
            args=(
                day_url,
                d,
                file_path,
                proxies,
                local_day_dict,
                network_day_dict,
            ),
        )
        t.start()
        t_list.append(t)

    if t_list:
        [t.join() for t in t_list]

    # https://data.binance.vision/data/spot/monthly/klines/BCCBTC/1d/BCCBTC-1d-2018-11.zip
    download_urls = list(network_month_dict.keys()) + list(network_day_dict.keys())
    load_urls = (
        list(local_month_dict.keys()) + list(local_day_dict.keys()) + download_urls
    )
    return load_urls, download_urls


def get_urls_by_xml_parse(
    data_type: DataType,
    asset_type: AssetType,
    symbol: str,
    start: datetime,
    end: datetime,
    timeframe: TIMEFRAMES,
    file_path: Path,
    proxies: dict[str, str] = {},
) -> tuple[list[str], list[str]]:
    assert start <= end, "start cannot be greater than end"
    all_dict = {}

    month_url_dict = {}
    local_month_dict = {}
    network_month_dict = {}
    local_day_list = []
    network_day_list = []
    if data_type in timeframe_data_types_dict["1M"]:
        months = pd.date_range(
            start.replace(day=1),
            end,
            freq="MS",
        ).to_list()

        month_url_dict = {
            gen_data_url(
                data_type, asset_type, Freq.monthly, symbol, m, timeframe=timeframe
            ): m
            for m in months
        }

        if month_url_dict:
            month_url_list_remote = loop_get_url_list_remote(
                "monthly", asset_type, data_type, symbol, timeframe, proxies
            )

            for month_url, m in month_url_dict.items():
                if get_local_data_path(month_url, file_path).exists():
                    local_month_dict[month_url] = m
                elif month_url in month_url_list_remote:
                    network_month_dict[month_url] = m

            all_dict = local_month_dict | network_month_dict
            if all_dict:
                all_keys = list(all_dict.keys())
                all_keys.sort()
                start = all_dict[all_keys[-1]] + pd.DateOffset(months=1)
            else:
                start = months[-1]

    if data_type in timeframe_data_types_dict["1d"]:
        days_url_list = [
            gen_data_url(
                data_type, asset_type, Freq.daily, symbol, d, timeframe=timeframe
            )
            for d in pd.date_range(start, end, freq="D").to_list()
        ]

        if days_url_list:
            days_url_list_remote = loop_get_url_list_remote(
                "daily", asset_type, data_type, symbol, timeframe, proxies
            )

            for day_url in days_url_list:
                if get_local_data_path(day_url, file_path).exists():
                    local_day_list.append(day_url)
                elif day_url in days_url_list_remote:
                    network_day_list.append(day_url)

    # https://data.binance.vision/data/spot/monthly/klines/BCCBTC/1d/BCCBTC-1d-2018-11.zip
    download_urls = list(network_month_dict.keys()) + network_day_list
    load_urls = list(local_month_dict.keys()) + local_day_list + download_urls
    return load_urls, download_urls


def loop_get_url_list_remote(
    freq: Literal["daily", "monthly"],
    asset_type: AssetType,
    data_type: DataType,
    symbol: str,
    timeframe: TIMEFRAMES,
    proxies: dict[str, str],
) -> list[str]:
    prefix = f"data/{asset_type.value}/{freq}/{data_type.value}/{symbol}/"
    params = {
        "delimiter": r"/",
        "prefix": prefix + f"{timeframe}/"
        if data_type in [DataType.klines]
        else prefix,
    }
    is_truncated, day_url_list_remote = get_vision_data_url_list(params, proxies)
    while is_truncated:
        params["marker"] = (day_url_list_remote[-1] + ".CHECKSUM").replace(
            "https://data.binance.vision/", ""
        )

        is_truncated, temp_day_url_list_remote = get_vision_data_url_list(
            params, proxies
        )
        day_url_list_remote += temp_day_url_list_remote

    return day_url_list_remote


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
        try:
            if ("klines" in path.parts) and (
                (int(path.stem.split("-")[2]) < 2022) or ("spot" in path.parts)
            ):
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
        except Exception as e:
            logger.error(f"load_data_from_disk error: {e}")
            return None

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
    month_urls, day_urls = get_urls_by_threading_ping(
        data_type=DataType.klines,
        asset_type=AssetType.future_um,
        symbol="BTCUSDT",
        start=start,
        end=end,
        timeframe="1m",
        file_path=Path("."),
    )
    print(1)
