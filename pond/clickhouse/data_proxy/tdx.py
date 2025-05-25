from datetime import datetime

import pandas as pd
import akshare as ak
from loguru import logger
from mootdx.reader import Reader

from pond.akshare.bond.all_basic import get_bond_basic_df
from pond.clickhouse import TsTable
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import (
    BondKline5m,
    KlineDailyNFQ,
    StockKline5m,
)
from pond.enums import Adjust, Interval, Product


class TdxDataProxy(DataProxy):
    tdx_reader: Reader = None

    def __init__(self, tdx_path: str) -> None:
        self.reader = Reader.factory(market="std", tdxdir=tdx_path)

    def get_table(
        self,
        interval: Interval,
        adjust: Adjust,
        product: Product = Product.STOCK,
    ) -> TsTable | None:
        return {
            (Interval.MINUTE_5, Adjust.NFQ, Product.STOCK): StockKline5m,
            (Interval.DAY_1, Adjust.NFQ, Product.STOCK): KlineDailyNFQ,
            (Interval.MINUTE_5, Adjust.NFQ, Product.BOND): BondKline5m,
        }[(interval, adjust, product)]

    def get_symobls(self, product: Product = Product.STOCK) -> list[str]:
        if product == Product.BOND:
            # 1. 获取全部债券信息, 包含退市债券
            bond_zh_cov_df = ak.bond_zh_cov()
            symbols = bond_zh_cov_df["债券代码"].to_list()
        elif product == Product.STOCK:
            stocks_df = ak.stock_info_a_code_name()
            symbols = stocks_df["code"].to_list()
        return symbols

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


if __name__ == "__main__":
    tdx_path = r"E:\new_tdx"
    data_proxy = TdxDataProxy(tdx_path)
    bond_klines_df = data_proxy.get_symobls(Product.BOND)
    stock_klines_df = data_proxy.get_symobls(Product.STOCK)
    pass
