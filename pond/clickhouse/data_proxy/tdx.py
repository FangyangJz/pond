from datetime import datetime

import pandas as pd
import akshare as ak
from loguru import logger
from mootdx.reader import Reader

from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import StockKline15m, StockKline5m
from pond.enums import Adjust, Interval


class TdxDataProxy(DataProxy):
    tdx_reader: Reader = None

    def __init__(self, tdx_path: str) -> None:
        self.reader = Reader.factory(market="std", tdxdir=tdx_path)

    def get_table(
        self, interval: Interval, adjust: Adjust
    ) -> StockKline5m | StockKline15m | None:
        if adjust in ["nfq", "", "3"]:
            if interval == "5m":
                return StockKline5m
            elif interval == "15m":
                return StockKline15m
        return None

    def get_symobls(self) -> list[str]:
        stocks_df = ak.stock_info_a_code_name()
        return stocks_df["code"].to_list()

    def get_klines(
        self,
        symbol: str,
        period: Interval,
        start: datetime = None,
        end: datetime = None,
        adjust: Adjust = Adjust.NFQ,
        limit: int = 1000,
    ) -> pd.DataFrame:
        if adjust is not Adjust.NFQ:
            logger.warning('TdxDataProxy current only support "nfq" adjust')

        if period in ["5", "5m", "15", "15m"]:
            suffix = 5
        elif period in ["1", "1m"]:
            suffix = 1
        df: pd.DataFrame = None
        if period.endswith("d"):
            df = self.reader.daily(symbol=symbol)
        else:
            df = self.reader.minute(symbol=symbol, suffix=suffix)

        if df is not None:
            if start is not None:
                df = df[start:]
            if end is not None:
                df = df[:end]
            if period == "15m":
                df = self.agg_kline_to15(df)
            df = df.reset_index()
        return df

    def agg_kline_to15(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.resample(rule="15min", closed="right", label="right")
            .agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "amount": "sum",
            })
            .dropna()
        )
        return df
