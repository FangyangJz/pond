# !/usr/bin/env python3
# @Datetime : 2024/3/4 下午 03:13
# @Author   : Fangyang
# @Software : PyCharm

import polars as pl
from pond.binance_history.type import DataType

klines_schema = {
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
    "ignore": pl.Float64,
}

metrics_schema = {
    "create_time": pl.String,
    "symbol": pl.String,
    "sum_open_interest": pl.Float64,  # 持仓总数量
    "sum_open_interest_value": pl.Float64,  # 持仓总价值
    "count_toptrader_long_short_ratio": pl.Float64,  # 大户多空比例
    "sum_toptrader_long_short_ratio": pl.Float64,
    "count_long_short_ratio": pl.Float64,
    "sum_taker_long_short_vol_ratio": pl.Float64,
}

fundingRate_schema = {
    "calc_time": pl.Int64,
    "funding_interval_hours": pl.Int8,
    "last_funding_rate": pl.Float64,
}

timeframe_data_types_dict = {
    "1m": [DataType.klines],
    "5m": [DataType.klines],
    "15m": [DataType.klines],
    "30m": [DataType.klines],
    "1h": [DataType.klines],
    "1d": [DataType.klines, DataType.trades, DataType.aggTrades, DataType.metrics],
    "1M": [DataType.klines, DataType.trades, DataType.aggTrades, DataType.fundingRate],
}

if __name__ == "__main__":
    pass
