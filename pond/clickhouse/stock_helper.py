import os
import math
import time
from datetime import datetime
from datetime import timedelta
import threading

from loguru import logger
import polars as pl
from pond.clickhouse import TsTable
from pond.clickhouse.data_proxy import DataProxy
from pond.clickhouse.data_proxy.tdx import TdxDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.enums import Interval, Adjust, Product
from pond.utils.times import (
    datetime2utctimestamp_milli,
    timeframe2minutes,
)


class StockHelper:
    clickhouse: ClickHouseManager = None
    data_proxy: DataProxy = None
    fix_data: bool = False
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
        self,
        table: TsTable,
        code_col: str = "code",
        time_col: str = "datetime",
        sync_time: datetime | None = None,
    ):
        if sync_time is None:
            sync_time = self.clickhouse.get_latest_record_time(table)
        latest_records_df = self.clickhouse.native_read_table(
            table,
            start_date=sync_time - timedelta(seconds=1),
            end_date=sync_time,
        )
        latest_synced_codes = (
            [] if latest_records_df is None else latest_records_df[code_col].unique()
        )
        return latest_synced_codes

    def sync_kline(
        self,
        interval: Interval,
        adjust: Adjust,
        product=Product.STOCK,
        workers: int = 0,
        end_time: datetime | None = None,
        time_col: str = "datetime",
    ) -> bool:
        table = self.data_proxy.get_table(interval, adjust, product)
        if table is None:
            return False

        signal = end_time if end_time is not None else datetime.now()
        signal = signal.replace(hour=15, minute=0, second=0, microsecond=0)
        workers = math.ceil(os.cpu_count() / 2) if workers <= 0 else workers

        symbols = self.data_proxy.get_symobls()
        if self.fix_data:
            latest_synced_codes = self.get_synced_codes(table, sync_time=signal)
            symbols = [s for s in symbols if s not in latest_synced_codes]
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
        latest_kline_df = self.clickhouse.native_read_table(
            table=table,
            start_date=signal - timedelta(minutes=timeframe2minutes(interval) * 2),
            end_date=signal,
            filters=None,
            rename=True,
        )
        if latest_kline_df is None or len(latest_kline_df) == 0:
            logger.warning(
                f"stock helper sync kline for {signal} into {table} failed, latest kline df is empty."
            )
            return False
        latest_kline_df = pl.from_pandas(latest_kline_df)
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

        latest_klines_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        latest_klines_df = pl.from_pandas(latest_klines_df)

        for i in range(0, len(symbols)):
            logger.debug(
                f"stock helper sync kline counting {i}/{len(symbols)} for {signal}"
            )
            symbol = symbols[i]
            time.sleep(0.1)
            latest_record = latest_klines_df.filter(pl.col("code") == symbol)
            lastest_record = (
                latest_record[0, "datetime"] if len(latest_record) > 0 else sync_start
            )
            if lastest_record.date() == signal.date():
                logger.debug(
                    f"stock helper sync kline ignore too short duration {lastest_record}-{signal}"
                )
                continue
            try:
                klines_df = self.data_proxy.get_klines(
                    symbol=symbol,
                    period=interval,
                    adjust=adjust,
                    start=lastest_record,
                    end=signal,
                    limit=1000,
                )
            except Exception as e:
                logger.error(f"stock helper sync kline failed for {symbol} {e}")
                klines_df = None
            if klines_df is not None and len(klines_df) > 0:
                klines_df["code"] = symbol
                if "turn" not in klines_df.columns:
                    klines_df["turn"] = None
                klines_df = klines_df.rename(
                    mapper={"date": "datetime"}, axis="columns"
                )
                klines_df = klines_df.drop_duplicates(subset=["datetime"])
                self.clickhouse.save_dataframe(table.__tablename__, klines_df)
        res_dict[tid] = True


if __name__ == "__main__":
    import os
    from pond.clickhouse.data_proxy.baostock import BaostockDataProxy

    tdx_path = None  # r"D:\windows\programs\TongDaXin"
    host = os.environ.get("CLICKHOUSE_HOST").strip()
    password = os.environ.get("CLICKHOUSE_PWD").strip()
    conn_str = f"clickhouse://default:{password}@{host}:8123/quant"
    native_conn_str = (
        f"clickhouse+native://default:{password}@{host}:9000/quant?tcp_keepalive=true"
    )
    # 此处有天坑，原因是同步机制中增加了start-end限制，避免接口返回异常，如果部分标的再start附近没有数据，start就不会更新，再次查询还是同样的start-end,就下载不了数据。
    # 没想到好的办法解决，在以这个时间为起始时间全部同步完之后，按每次增加1年再同步一遍。
    sync_start = datetime(2015, 1, 5)
    manager = ClickHouseManager(
        conn_str, data_start=sync_start, native_uri=native_conn_str
    )
    helper = StockHelper(manager, tdx_path=tdx_path)
    helper.fix_data = True
    helper.sync_data_start = None  # sync_start  # datetime(2024, 10, 31, 15)
    ret = False
    sync_end = datetime.now()
    sync_end = datetime(2026, 2, 13)
    logger.info(f"sync at {sync_start} start")
    data_proxy = BaostockDataProxy(sync_stock_list_date=sync_end)
    data_proxy.min_sync_interval_days = 0
    data_proxy.min_start_date = helper.sync_data_start
    helper.set_data_proxy(data_proxy)
    while sync_end > datetime(2020, 1, 1):
        ret = helper.sync_kline(
            interval=Interval.MINUTE_5,
            adjust=Adjust.NFQ,
            workers=1,
            end_time=sync_end,
        )
        sync_end -= timedelta(days=1)
    logger.info(f"sync at {sync_start} end")
