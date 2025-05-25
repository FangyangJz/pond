import os
import math
import time
from datetime import datetime
import threading

from pond.clickhouse import TsTable
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.data_proxy.tdx import TdxDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.kline import BaoStockKline5m
from loguru import logger
from datetime import timedelta
from pond.enums import Interval, Adjust
from pond.utils.times import (
    datetime2utctimestamp_milli,
    timeframe2minutes,
)


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
        interval: Interval,
        adjust: Adjust,
        workers: int = 0,
        end_time: datetime | None = None,
        time_col: str = "datetime",
    ) -> bool:
        table = self.data_proxy.get_table(interval, adjust)
        if table is None:
            return False

        signal = (
            end_time
            if end_time is not None
            else datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)
        )
        workers = math.ceil(os.cpu_count() / 2) if workers <= 0 else workers

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
        threads: list[threading.Thread] = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = threading.Thread(
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
        self,
        signal: datetime,
        table: TsTable,
        symbols: list[str],
        interval: Interval,
        adjust: Adjust,
        res_dict: dict[int, bool],
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False

        if signal is None:
            signal = datetime.now().replace(hour=15, minute=0, second=0, microsecond=0)

        for symbol in symbols:
            time.sleep(0.1)
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == symbol
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()
            if data_duration_seconds < interval.seconds:
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
    from pond.clickhouse.data_proxy.baostock import BaostockDataProxy

    tdx_path = None  # r"D:\windows\programs\TongDaXin"
    # password = os.environ.get("CLICKHOUSE_PWD")
    password = ""
    conn_str = f"clickhouse://default:{password}@localhost:18123/quant"
    native_conn_str = f"clickhouse+native://default:{password}@localhost:18123/quant?tcp_keepalive=true"
    sync_start = datetime(2020, 1, 2)
    manager = ClickHouseManager(
        conn_str, data_start=sync_start, native_uri=native_conn_str
    )
    helper = StockHelper(manager, tdx_path=tdx_path)
    helper.fix_data = True
    helper.sync_data_start = sync_start  # datetime(2024, 10, 31, 15)
    while sync_start < datetime.now().replace(hour=0).replace(minute=0) or True:
        sync_start = manager.get_latest_record_time(BaoStockKline5m)
        print(f"sync at {sync_start} start")
        data_proxy = BaostockDataProxy(sync_stock_list_date=sync_start)
        data_proxy.min_sync_interval_days = 5
        data_proxy.min_start_date = helper.sync_data_start
        helper.set_data_proxy(data_proxy)
        ret = helper.sync_kline(
            interval="5m",
            adjust="1",  # '1' 后复权, '2' 前复权, '3' 不复权
            workers=1,
            end_time=datetime.now().replace(hour=0).replace(minute=0),
        )
        sync_start = sync_start + timedelta(days=1)
        print(f"sync at {sync_start} end")
