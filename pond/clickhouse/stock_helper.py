import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
from datetime import datetime
from pond.clickhouse.kline import StockKline5m, StockKline15m, BaoStockKline5m
from threading import Thread
import threading
import pandas as pd
from loguru import logger
from datetime import timedelta
from pond.utils.times import (
    datetime2utctimestamp_milli,
    timeframe2minutes,
)
from mootdx.reader import Reader
import akshare as ak
import baostock as bs
import time


class DataProxy:
    def get_table(self, interval, adjust):
        pass

    def get_symobls(self) -> list[str]:
        pass

    def get_klines(
        self,
        symbol: str,
        period: str,
        adjust: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        pass


class TdxDataProxy(DataProxy):
    tdx_reader: Reader = None

    def __init__(self, tdx_path: str) -> None:
        self.reader = Reader.factory(market="std", tdxdir=tdx_path)

    def get_table(self, interval, adjust) -> Optional[StockKline5m]:
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
        period: str,
        adjust: str,
        start: datetime = None,
        end: datetime = None,
        limit: int = 1000,
    ):
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

    def agg_kline_to15(self, df: pd.DataFrame):
        df = (
            df.resample(rule="15min", closed="right", label="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                }
            )
            .dropna()
        )
        return df


class BaostockDataProxy(DataProxy):
    sync_stock_list_date: datetime = None
    min_sync_interval_days = 1
    min_start_date = None

    def __init__(self, sync_stock_list_date) -> None:
        super().__init__()
        self.sync_stock_list_date = sync_stock_list_date
        lg = bs.login()
        logger.info("login respond error_code:" + lg.error_code)
        logger.info("login respond  error_msg:" + lg.error_msg)

    def get_table(self, interval, adjust) -> Optional[StockKline5m]:
        if adjust in ["nfq", "", "3"]:
            if interval in ["5", "5m"]:
                return BaoStockKline5m
        return None

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

    def get_klines(
        self,
        symbol: str,
        period: str,
        adjust: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        # baostock always return data include start day but already existed.
        start += timedelta(days=1)
        if period.endswith("m"):
            period = period[:-1]
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
            "date,time,code,open,high,low,close,volume,amount,adjustflag",
            start_date=start_date,
            end_date=end_date,
            frequency=period,
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


class StockHelper:
    clickhouse: ClickHouseManager = None
    data_proxy: DataProxy = None
    fix_data = False
    sync_data_start: datetime = None

    def __init__(
        self, clickhouse: ClickHouseManager, tdx_path: str = None, **kwargs
    ) -> None:
        self.clickhouse = clickhouse
        self.configs = kwargs
        if tdx_path is not None:
            self.data_proxy = TdxDataProxy(tdx_path)

    def set_data_proxy(self, data_proxy: DataProxy):
        self.data_proxy = data_proxy

    def gen_stub_kline_as_list(self, start: datetime, end: datetime):
        open_time_mill = datetime2utctimestamp_milli(start)
        close_time_mill = datetime2utctimestamp_milli(end)
        stub_list = [
            open_time_mill,
            0,
            0,
            0,
            0,
            0,
            close_time_mill,
            0,
            0,
            0,
            0,
        ]
        return stub_list

    def get_synced_codes(
        self, table, code_col="code", time_col="datetime", sync_time: datetime = None
    ):
        if sync_time is None:
            sync_time = self.clickhouse.get_latest_record_time(table)
        latest_records_df = self.clickhouse.native_read_table(
            table,
            start_date=sync_time - timedelta(seconds=1),
            end_date=sync_time,
        )
        latest_synced_codes = latest_records_df[code_col].unique()
        return latest_synced_codes

    def sync_kline(
        self,
        interval,
        adjust,
        workers=None,
        end_time: datetime = None,
        time_col="datetime",
    ) -> bool:
        table = self.data_proxy.get_table(interval, adjust)
        if table is None:
            return False
        if end_time is not None:
            signal = end_time
        else:
            signal = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
        if workers is None:
            workers = math.ceil(os.cpu_count() / 2)
        symbols = self.data_proxy.get_symobls()
        if self.fix_data:
            latest_synced_codes = self.get_synced_codes(table)
            symbols = [s for s in symbols if s not in latest_synced_codes]
        if self.sync_data_start is not None:
            available_codes = self.get_synced_codes(
                table, sync_time=self.sync_data_start
            )
            symbols = [s for s in symbols if s in available_codes]
        task_counts = math.ceil(len(symbols) / workers)
        res_dict = {}
        threads: list[Thread] = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = Thread(
                target=self.__sync_kline,
                args=(signal, table, worker_symbols, interval, adjust, res_dict),
            )
            worker.start()
            threads.append(worker)

        [t.join() for t in threads]

        for tid in res_dict.keys():
            if not res_dict[tid]:
                # return false if any thread failed.
                return False

        request_count = len(symbols)
        latest_kline_df = self.clickhouse.read_table(
            table,
            signal - timedelta(minutes=timeframe2minutes(interval) * 2),
            signal,
            filters=None,
            rename=True,
        )
        if len(latest_kline_df) == 0:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} failed, latest kline df is empty."
            )
            return False
        lastest_count = (
            latest_kline_df.group_by(time_col).count().sort(time_col)[-1, "count"]
        )
        if lastest_count / request_count < 0.98:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} failed, lastest count {lastest_count} request count {request_count}"
            )
            return False
        return True

    def __sync_kline(
        self, signal, table: StockKline5m, symbols, interval, adjust, res_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
        for symbol in symbols:
            time.sleep(0.1)
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == symbol
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()
            if data_duration_seconds < interval_seconds:
                logger.debug(
                    f"stock helper sync kline ignore too short duration {lastest_record}-{signal}"
                )
                continue
            klines_df = self.data_proxy.get_klines(
                symbol=symbol,
                period=interval,
                adjust=adjust,
                start=lastest_record,
                end=None,
                limit=1000,
            )
            if klines_df is not None and len(klines_df) > 0:
                klines_df["code"] = symbol
                if "turn" not in klines_df.columns:
                    klines_df["turn"] = None
                klines_df = klines_df.rename(
                    mapper={"date": "datetime"}, axis="columns"
                )
                klines_df = klines_df.drop_duplicates(subset=["datetime"])
                self.clickhouse.save_to_db(table, klines_df, table.code == symbol)
        res_dict[tid] = True


if __name__ == "__main__":
    import os

    tdx_path = r"D:\windows\programs\TongDaXin"
    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    native_conn_str = f"clickhouse+native://default:{password}@localhost:9000/quant?tcp_keepalive=true"
    sync_start = datetime(2020, 1, 1)
    manager = ClickHouseManager(
        conn_str, data_start=sync_start, native_uri=native_conn_str
    )
    helper = StockHelper(manager, tdx_path=tdx_path)
    helper.fix_data = True
    helper.sync_data_start = datetime(2024, 10, 31, 15)
    while sync_start < datetime.now().replace(hour=0).replace(minute=0) or True:
        sync_start = manager.get_latest_record_time(BaoStockKline5m)
        print(f"sync at {sync_start} start")
        data_proxy = BaostockDataProxy(sync_stock_list_date=sync_start)
        data_proxy.min_sync_interval_days = 5
        data_proxy.min_start_date = helper.sync_data_start
        helper.set_data_proxy(data_proxy)
        ret = helper.sync_kline(
            interval="5m",
            adjust="3",
            workers=1,
            end_time=datetime.now().replace(hour=0).replace(minute=0),
        )
        sync_start = sync_start + timedelta(days=1)
        print(f"sync at {sync_start} end")
