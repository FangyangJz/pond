import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
import datetime as dtm
from datetime import datetime
from binance.um_futures import UMFutures
from pond.clickhouse.kline import FuturesKline5m, FuturesKline1H
from threading import Thread
import threading
import pandas as pd
from loguru import logger
from pathlib import Path
from datetime import timedelta
from pond.duckdb.crypto import CryptoDB
from pond.utils.times import (
    datetime2utctimestamp_milli,
    utcstamp_mill2datetime,
    timeframe2minutes,
)


class FuturesHelper:
    crypto_db: CryptoDB = None
    clickhouse: ClickHouseManager = None
    dict_exchange_info = {}
    exchange: UMFutures = None

    def __init__(
        self, crypto_db: CryptoDB, clickhouse: ClickHouseManager, **kwargs
    ) -> None:
        self.crypto_db = crypto_db
        self.clickhouse = clickhouse
        self.exchange = UMFutures()
        self.configs = kwargs

    def get_exchange_info(self, signal: datetime):
        key = str(signal.date())
        if key in self.dict_exchange_info.keys():
            return self.dict_exchange_info[key]
        dict_exchange_info = self.exchange.exchange_info()
        self.dict_exchange_info = {key: dict_exchange_info}
        return self.dict_exchange_info[key]

    def get_perpetual_symbols(self, signal: datetime):
        exchange_info_dict = self.get_exchange_info(signal)
        return [
            symbol
            for symbol in exchange_info_dict["symbols"]
            if symbol["contractType"] == "PERPETUAL"
        ]

    def get_futures_table(self, interval) -> Optional[FuturesKline1H]:
        if interval == "1h":
            return FuturesKline1H
        if interval == "5m":
            return FuturesKline5m
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

    def sync_futures_kline(
        self, interval, workers=None, end_time: datetime = None
    ) -> bool:
        table = self.get_futures_table(interval)
        if table is None:
            return False
        if end_time is not None:
            signal = end_time
        else:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        if workers is None:
            workers = math.ceil(os.cpu_count() / 2)
        symbols = self.get_perpetual_symbols(signal)
        task_counts = math.ceil(len(symbols) / workers)
        res_dict = {}
        threads = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = Thread(
                target=self.__sync_futures_kline,
                args=(signal, table, worker_symbols, interval, res_dict),
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
        # current about 300 futues, allow 5 target kline missing.
        if lastest_count / request_count < 0.98:
            return False
        return True

    def __sync_futures_kline(self, signal, table, symbols, interval, res_dict: dict):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        data_limit = 720  # 1 month, max 1000
        limit_seconds = data_limit * timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        for symbol in symbols:
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()

            # load history data and save into db
            if data_duration_seconds > limit_seconds:
                local_klines_df = self.crypto_db.load_history_data(
                    code, lastest_record, signal, timeframe=interval, **self.configs
                )
                self.clickhouse.save_to_db(
                    table, local_klines_df.to_pandas(), table.code == code
                )
                # refresh data duration
                lastest_record = self.clickhouse.get_latest_record_time(
                    table, table.code == code
                )
                data_duration_seconds = (signal - lastest_record).total_seconds()

            if data_duration_seconds < interval_seconds:
                logger.debug(
                    f"futures helper sync kline ignore too short duration {lastest_record}-{signal}"
                )
                continue

            startTime = datetime2utctimestamp_milli(signal) - limit_seconds * 1000
            klines_list = self.exchange.continuous_klines(
                code,
                "PERPETUAL",
                interval,
                startTime=startTime,
                limit=1000,
            )
            if not klines_list:
                # generate stub kline to mark latest sync time.
                klines_list = [self.gen_stub_kline_as_list(lastest_record, signal)]
            cols = list(table().get_colcom_names().values())[1:] + ["stub"]
            klines_df = pd.DataFrame(klines_list, columns=cols)
            klines_df["code"] = code
            klines_df["open_time"] = klines_df["open_time"].apply(
                utcstamp_mill2datetime
            )
            klines_df["close_time"] = klines_df["close_time"].apply(
                utcstamp_mill2datetime
            )
            klines_df = klines_df[
                klines_df["close_time"] <= datetime.now(tz=dtm.timezone.utc)
            ]
            klines_df = klines_df.drop_duplicates(subset=["close_time"])
            self.clickhouse.save_to_db(table, klines_df, table.code == code)
        res_dict[tid] = True


if __name__ == "__main__":
    import os

    crypto_db = CryptoDB(Path(r"E:\DuckDB"))
    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    native_conn_str = f"clickhouse+native://default:{password}@localhost:9000/quant?tcp_keepalive=true"
    manager = ClickHouseManager(
        conn_str, data_start=datetime(2020, 1, 1), native_uri=native_conn_str
    )
    helper = FuturesHelper(crypto_db, manager)
    ret = helper.sync_futures_kline(
        "5m", workers=1, end_time=datetime.now().replace(hour=0).replace(minute=0)
    )
    print(f"sync ret {ret}")
