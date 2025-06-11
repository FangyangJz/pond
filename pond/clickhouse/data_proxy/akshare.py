from datetime import datetime, timedelta

from loguru import logger
import pandas as pd
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import FundKlineDailyHFQ, FundNetValue
from pond.enums import Adjust, Interval, Product
from pond.utils.times import timeit_cls_method_wrapper
import akshare as ak


class AKShareFundDataProxy(DataProxy):
    sync_codes_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None

    def __init__(self) -> None:
        super().__init__()

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> FundKlineDailyHFQ:
        return {
            (Interval.DAY_1, Adjust.HFQ, Product.FUND): FundKlineDailyHFQ,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        df = ak.fund_name_em()
        df = df[df["基金简称"].str.contains("ETF")]
        return df["基金代码"].tolist()

    def get_frequency(self, interval: Interval) -> str:
        if interval == Interval.MINUTE_5:
            return "5"
        elif interval == Interval.MINUTE_15:
            return "15"
        elif interval == Interval.MINUTE_30:
            return "30"
        elif interval == Interval.HOUR_1:
            return "60"
        elif interval == Interval.DAY_1:
            return "d"
        elif interval == Interval.WEEK_1:
            return "w"
        elif interval == Interval.MONTH_1:
            return "m"
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    @timeit_cls_method_wrapper
    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        if end is None:
            end = start + timedelta(days=365 * 3)
        if end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        df = ak.fund_etf_hist_em(
            symbol=symbol,
            period=period.toAKshare(),
            start_date=start_date,
            end_date=end_date,
            adjust=adjust.toAKshare(),
        )
        if len(df) == 0:
            return df
        df["日期"] = pd.to_datetime(df["日期"])
        df["date"] = df["日期"]
        df["turn"] = df["换手率"]
        return df


class AKShareFundNetValueProxy(DataProxy):
    sync_codes_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None
    downloading_batch_days = 365 * 3

    def __init__(self) -> None:
        super().__init__()

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> FundNetValue:
        return {
            (Interval.DAY_1, Adjust.HFQ, Product.FUND): FundNetValue,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        df = ak.fund_name_em()
        df = df[df["基金简称"].str.contains("ETF")]
        return df["基金代码"].tolist()

    def get_frequency(self, interval: Interval) -> str:
        if interval == Interval.MINUTE_5:
            return "5"
        elif interval == Interval.MINUTE_15:
            return "15"
        elif interval == Interval.MINUTE_30:
            return "30"
        elif interval == Interval.HOUR_1:
            return "60"
        elif interval == Interval.DAY_1:
            return "d"
        elif interval == Interval.WEEK_1:
            return "w"
        elif interval == Interval.MONTH_1:
            return "m"
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        while start < datetime.now() - timedelta(days=self.min_sync_interval_days):
            logger.debug(f"get_klines start downloading for {symbol} at {start}")
            try:
                df = self.__get_klines(symbol, period, adjust, start, end, limit)
            except Exception:
                df = None
            if df is None or len(df) == 0:
                start += timedelta(days=self.downloading_batch_days)
                if end is not None:
                    end += timedelta(days=self.downloading_batch_days)
                continue
            return df
        return None

    @timeit_cls_method_wrapper
    def __get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        if end is None:
            end = start + timedelta(days=self.downloading_batch_days)
        if end > datetime.now():
            end = datetime.now()
        if end - start < timedelta(days=self.min_sync_interval_days):
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        if self.min_start_date is not None and start < self.min_start_date:
            logger.debug(
                f"get_klines {symbol} from {start} to {end} data len {0}, skip"
            )
            return None
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        df = ak.fund_etf_fund_info_em(
            fund=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if len(df) == 0:
            return df
        df["date"] = pd.to_datetime(df["净值日期"])
        df["tradable"] = (df["申购状态"] == "开放申购") & (df["赎回状态"] == "开放赎回")
        return df
