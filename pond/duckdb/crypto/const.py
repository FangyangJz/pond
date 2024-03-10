# !/usr/bin/env python3
# @Datetime : 2024/3/4 下午 03:13
# @Author   : Fangyang
# @Software : PyCharm

import polars as pl

kline_schema = {
    "open_time": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "close_time": pl.Int64,
    "quote_volume": pl.Float64,
    "count": pl.Int64,
    "taker_buy_volume": pl.Float64,
    "taker_buy_quote_volume": pl.Float64,
    "ignore": pl.Int8,
}

if __name__ == "__main__":
    pass
