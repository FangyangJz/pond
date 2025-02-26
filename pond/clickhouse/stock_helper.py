import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
import datetime as dtm
from datetime import datetime
from pond.clickhouse.kline import StockKline5m, StockKline15m
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


class DataProxy:
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


class StockHelper:
    clickhouse: ClickHouseManager = None
    data_proxy: DataProxy = None

    def __init__(
        self, clickhouse: ClickHouseManager, tdx_path: str = None, **kwargs
    ) -> None:
        self.clickhouse = clickhouse
        self.configs = kwargs
        if tdx_path is not None:
            self.data_proxy = TdxDataProxy(tdx_path)

    def set_data_proxy(self, data_proxy: DataProxy):
        self.data_proxy = data_proxy

    def get_table(self, interval) -> Optional[StockKline5m]:
        if interval == "5m":
            return StockKline5m
        elif interval == "15m":
            return StockKline15m
        return None

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

    def sync_kline(
        self, interval, adjust, workers=None, end_time: datetime = None
    ) -> bool:
        table = self.get_table(interval)
        if table is None:
            return False
        if end_time is not None:
            signal = end_time
        else:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        if workers is None:
            workers = math.ceil(os.cpu_count() / 2)
        symbols = self.data_proxy.get_symobls()
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
            return False
        lastest_count = (
            latest_kline_df.group_by("close_time")
            .count()
            .sort("close_time")[-1, "count"]
        )
        if lastest_count / request_count < 0.98:
            return False
        return True

    def __sync_kline(
        self, signal, table: StockKline5m, symbols, interval, adjust, res_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        for symbol in symbols:
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
    manager = ClickHouseManager(
        conn_str, data_start=datetime(2020, 1, 1), native_uri=native_conn_str
    )
    helper = StockHelper(manager, tdx_path=tdx_path)
    ret = helper.sync_kline(
        interval="5m",
        adjust="",
        workers=10,
        end_time=datetime.now().replace(hour=0).replace(minute=0),
    )
    print(f"sync ret {ret}")
