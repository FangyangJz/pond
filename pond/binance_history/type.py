# !/usr/bin/env python3
# @Datetime : 2023/12/23 下午 09:31
# @Author   : Fangyang
# @Software : PyCharm

from enum import Enum
from typing import Literal


TIMEFRAMES = Literal[
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]

TIMEZONE = Literal["UTC", "Asia/Shanghai"]


class Freq(Enum):
    monthly = "monthly"
    daily = "daily"


class DataType(Enum):
    klines = "klines"
    aggTrades = "aggTrades"


class AssetType(Enum):
    spot = "spot"
    future_um = "futures/um"
    future_cm = "futures/cm"


if __name__ == "__main__":
    assert AssetType.future_cm.value == "futures/cm"
