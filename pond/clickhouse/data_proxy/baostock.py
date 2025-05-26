from datetime import datetime, timedelta

from loguru import logger
import pandas as pd
import baostock as bs
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.kline import BaoStockKline5m
from pond.enums import Adjust, Interval, Product
from pond.utils.times import timeit_cls_method_wrapper


class BaostockDataProxy(DataProxy):
    sync_stock_list_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None

    def __init__(self, sync_stock_list_date: datetime) -> None:
        super().__init__()
        self.sync_stock_list_date = sync_stock_list_date
        lg = bs.login()
        logger.info("login respond error_code:" + lg.error_code)
        logger.info("login respond  error_msg:" + lg.error_msg)

    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> BaoStockKline5m:
        return {
            (Interval.MINUTE_5, Adjust.NFQ, Product.STOCK): BaoStockKline5m,
        }[(interval, adjust, product)]

    def get_symobls(self) -> list[str]:
        rs = bs.query_all_stock(day=self.sync_stock_list_date.strftime("%Y-%m-%d"))
        print("query_all_stock respond error_code:" + rs.error_code)
        print("query_all_stock respond  error_msg:" + rs.error_msg)
        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())
        stocks_df = pd.DataFrame(data_list, columns=rs.fields)
        stocks_df = stocks_df[~stocks_df["code_name"].str.endswith("指数")]
        return stocks_df["code"].to_list()

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

    def get_fields(self, interval: Interval) -> str:
        common_fields = (
            "date, time, code, open, high, low, close, volume, amount, adjustflag"
        )
        if interval in [
            Interval.MINUTE_5,
            Interval.MINUTE_15,
            Interval.MINUTE_30,
            Interval.HOUR_1,
        ]:
            return common_fields
        elif interval in [Interval.WEEK_1, Interval.MONTH_1]:
            return common_fields + ", turn, pctChg"
        elif interval == Interval.DAY_1:
            return (
                common_fields + ", turn, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST"
            )

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
        # baostock always return data include start day but already existed.
        start += timedelta(days=1)
        if end is None:
            end = start + timedelta(days=90)
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
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")

        rs = bs.query_history_k_data_plus(
            symbol,
            fields=self.get_fields(period),
            start_date=start_date,
            end_date=end_date,
            frequency=self.get_frequency(period),
            adjustflag=adjust,
        )
        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        logger.debug(
            f"query_history_k_data_plus {symbol} from {start_date} to {end_date} data len {len(result)} respond error_code: {rs.error_code}, msg {rs.error_msg}"
        )
        if len(result) == 0:
            return result
        result["datetime"] = result.apply(
            lambda row: datetime.strptime(row["time"][:-4], "%Y%m%d%H%M%S"), axis=1
        )
        return result.drop(columns=["date", "time", "adjustflag"], axis=1)
