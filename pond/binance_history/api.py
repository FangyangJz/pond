# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:29
# @Author   : Fangyang
# @Software : PyCharm

from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from pond.binance_history.type import TIMEFRAMES, AssetType, TIMEZONE, DataType
from pond.binance_history.utils import gen_download_urls, get_data, unify_datetime
from typing import Union, Dict


def fetch_data(
    symbol: str,
    asset_type: AssetType,
    data_type: DataType,
    start: Union[str, datetime],
    end: Union[str, datetime],
    tz: TIMEZONE = "UTC",
    timeframe: TIMEFRAMES = "1m",
    local_path: Union[Path, None] = None,
    proxies: Dict[str, str] = {},
) -> DataFrame:
    """
    :param symbol: The binance market pair name. e.g. ``'BTCUSDT'``.
    :param start:  The start datetime of requested data. If it's an instance of ``datetime.datetime``,
    :param asset_type: The asset type of requested data. It must be one of ``'spot'``, ``'futures/um'``, ``'futures/cm'``.
    :param data_type: The type of requested data. It must be one of ``'klines'``, ``'agg_trades'``.
        it's timezone is ignored. If it's a ``str``, it should be parsed by
        `dateutil <https://github.com/dateutil/dateutil>`_, e.g. ``"2022-1-1 8:10"``.
    :param end:  The end datetime of requested data. If it's an instance of ``datetime.datetime``,
        it's timezone is ignored. If it's a ``str``, it should be parsed by
        `dateutil <https://github.com/dateutil/dateutil>`_, e.g. ``"2022-1-2 8:10"``.
    :param tz: Timezone of ``start``, ``end``, and the open/close datetime of the returned dataframe.
        It should be a time zone name of `tz database <https://en.wikipedia.org/wiki/Tz_database>`_, e.g. "Asia/Shanghai".
        Your can find a full list of available time zone names in
        `List of tz database time zones <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List>`_.
    :param timeframe: The kline interval. e.g. "1m". see ``binance_history.constants.TIMEFRAMES``
        to see the full list of available intervals.
    :return: A pandas dataframe with columns `open`, `high`, `low`, `close`, `volume`, `trades`, `close_datetime`.
        the dataframe's index is the open datetime of klines, the timezone of the datetime is set by ``tz``,
        if it is None, your local timezone will be used.
    """

    start, end = unify_datetime(start), unify_datetime(end)
    start, end = pd.Timestamp(start, tz=tz), pd.Timestamp(end, tz=tz)
    symbol = symbol.upper().replace("/", "")

    months, days = gen_download_urls(
        data_type,
        asset_type,
        symbol,
        start.tz_convert(None),
        end.tz_convert(None),
        timeframe=timeframe,
        proxies=proxies
    )
    monthly_dfs = [
        get_data(
            data_type, asset_type, "monthly", symbol, dt, tz, timeframe, local_path
        )
        for dt in months
    ]
    daily_dfs = [
        get_data(data_type, asset_type, "daily", symbol, dt, tz, timeframe, local_path)
        for dt in days
    ]
    df = pd.concat(monthly_dfs + daily_dfs)  # type: ignore
    return df.loc[start:end]


if __name__ == "__main__":
    # check file link
    # https://data.binance.vision/?prefix=data/futures/cm/monthly/klines/
    from pathlib import Path

    symbol = "CVCUSDT"
    start = "2023-1-1"
    end = "2023-11-1"
    # tz = "Asia/Shanghai"
    tz = "UTC"

    klines = fetch_data(
        symbol=symbol,
        start=start,
        end=end,
        tz=tz,
        asset_type=AssetType.future_um,
        data_type=DataType.klines,
        # local_path=Path("/home/fangyang/zhitai5000/DuckDB/crypto/"),
        local_path=Path(r"E:/DuckDB/crypto/"),
    )
    print(1)
