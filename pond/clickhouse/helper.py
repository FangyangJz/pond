import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
import datetime as dtm
from datetime import datetime
from binance.um_futures import UMFutures
from pond.clickhouse.kline import (
    FutureInfo,
    FuturesKline5m,
    FuturesKline4H,
    FuturesKline1H,
    FuturesKline1d,
    FuturesKline15m,
)
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
from pond.coin_gecko.coin_info import get_coin_market_data
from pond.coin_gecko.id_mapper import CoinGeckoIDMapper
import polars as pl


class DataProxy:
    def um_future_exchange_info(self) -> dict:
        pass

    def um_future_klines(
        self, symbol, contract_type, interval, startTime, limit
    ) -> list:
        pass


class DirectDataProxy(DataProxy):
    exchange: UMFutures = None

    def __init__(self) -> None:
        self.exchange = UMFutures(
            proxies={"https": "127.0.0.1:7890", "http": "127.0.0.1:7890"}
        )

    def um_future_exchange_info(self) -> dict:
        return self.exchange.exchange_info()

    def um_future_klines(
        self, symbol, contract_type, interval, startTime, limit
    ) -> list:
        return self.exchange.continuous_klines(
            symbol,
            contract_type,
            interval,
            startTime=startTime,
            limit=1000,
        )


class FuturesHelper:
    crypto_db: CryptoDB = None
    clickhouse: ClickHouseManager = None
    dict_exchange_info = {}
    exchange: UMFutures = None
    data_proxy: DataProxy = None
    fix_kline_with_cryptodb: bool = None
    gecko_id_mapper: CoinGeckoIDMapper = CoinGeckoIDMapper()

    def __init__(
        self,
        crypto_db: CryptoDB,
        clickhouse: ClickHouseManager,
        fix_kline_with_cryptodb=True,
        **kwargs,
    ) -> None:
        self.crypto_db = crypto_db
        self.clickhouse = clickhouse
        self.configs = kwargs
        self.data_proxy = DirectDataProxy()
        self.fix_kline_with_cryptodb = fix_kline_with_cryptodb

    def set_data_prox(self, data_proxy: DataProxy):
        self.data_proxy = data_proxy

    def get_exchange_info(self, signal: datetime):
        key = str(signal.date())
        if key in self.dict_exchange_info.keys():
            return self.dict_exchange_info[key]
        self.dict_exchange_info.clear()
        dict_exchange_info = self.data_proxy.um_future_exchange_info()
        self.dict_exchange_info = {key: dict_exchange_info}
        return self.dict_exchange_info[key]

    def get_perpetual_symbols(self, signal: datetime):
        exchange_info_dict = self.get_exchange_info(signal)
        return [
            symbol
            for symbol in exchange_info_dict["symbols"]
            if symbol["contractType"] == "PERPETUAL" and symbol["pair"].endswith("USDT")
        ]

    def get_futures_table(self, interval) -> Optional[FuturesKline1H]:
        if interval == "4h":
            return FuturesKline4H
        if interval == "1h":
            return FuturesKline1H
        if interval == "5m":
            return FuturesKline5m
        if interval == "1d":
            return FuturesKline1d
        if interval == "15m":
            return FuturesKline15m
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
        ret = self.sync(interval, workers, end_time, sync_kline=True, sync_info=False)
        return ret

    def sync_futures_info(
        self, interval, workers=None, end_time: datetime = None
    ) -> bool:
        ret = self.sync(interval, workers, end_time, sync_kline=False, sync_info=True)
        return ret

    def sync(
        self,
        interval,
        workers=None,
        end_time: datetime = None,
        sync_kline=False,
        sync_info=False,
    ) -> bool:
        if sync_kline:
            table = self.get_futures_table(interval)
        else:
            table = FutureInfo
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
            if sync_kline:
                worker = Thread(
                    target=self.__sync_futures_kline,
                    args=(signal, table, worker_symbols, interval, res_dict),
                )
                worker.start()
                threads.append(worker)
            if sync_info:
                worker2 = Thread(
                    target=self.__sync_futures_info,
                    args=(signal, table, worker_symbols, res_dict),
                )
                worker2.start()
                threads.append(worker2)

        [t.join() for t in threads]

        for tid in res_dict.keys():
            if not res_dict[tid]:
                # return false if any thread failed.
                return False
        if sync_kline:
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
            if data_duration_seconds > limit_seconds and self.fix_kline_with_cryptodb:
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

            startTime = datetime2utctimestamp_milli(lastest_record)
            klines_list = self.data_proxy.um_future_klines(
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

    def __sync_futures_info(self, signal, table, symbols, res_dict: dict):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        count = 0
        for symbol in symbols:
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            if signal - lastest_record < timedelta(days=1):
                count += 1
                logger.info(
                    f"futures helper sync info ignore too short duration for {code}"
                )
                continue
            query = code[:-4]
            cg_id = self.gecko_id_mapper.get_coingecko_id(query, exact_match=True)
            if cg_id is None:
                logger.warning(
                    f"futures helper sync info can not find coin gecko id for {code}"
                )
                count += 1
                continue
            data = get_coin_market_data(cg_id)
            if data is None:
                continue
            total_supply = data.get("total_supply", None)
            market_cap_fdv_ratio = data.get("market_cap_fdv_ratio", None)
            self.clickhouse.save_to_db(
                table,
                pd.DataFrame(
                    {
                        "datetime": [signal],
                        "code": [code],
                        "total_supply": [total_supply],
                        "market_cap_fdv_ratio": [market_cap_fdv_ratio],
                    }
                ),
                table.code == code,
            )
            count += 1
        res_dict[tid] = count == len(symbols)

    def attach_future_info(self, df: pl.DataFrame, back_fill=True):
        start = df["close_time"].min()
        end = df["close_time"].max()
        # 限制扫描时间
        latest_time = self.clickhouse.get_latest_record_time(FutureInfo)
        latest_time = min(latest_time, start) - timedelta(days=365)
        # 获取每个标的最后一个记录时间
        last_1_df = self.clickhouse.read_latest_n_record(
            FutureInfo.__tablename__, latest_time, datetime.now()
        )
        latest_time = last_1_df["datetime"].min()
        # 读取数据
        info_df = self.clickhouse.native_read_table(
            FutureInfo,
            min(latest_time, start),
            datetime.now(),
            filters=None,
            rename=True,
        )
        info_df = pl.from_pandas(info_df).with_columns(
            date=pl.col("datetime").dt.date(),
            market_cap_fdv_ratio=pl.col("market_cap_fdv_ratio").fill_null(1),
        )
        df = df.with_columns(date=pl.col("close_time").dt.date())
        base_col_df = pl.concat(
            [info_df.select(["jj_code", "date"]), df.select(["jj_code", "date"])]
        ).unique(subset=["jj_code", "date"])
        df = base_col_df.join(
            df,
            on=["jj_code", "date"],
            how="left",
        )
        df = df.join(
            info_df,
            on=["jj_code", "date"],
            how="left",
        ).sort(["jj_code", "date", "close_time"])
        if back_fill:
            df = df.with_columns(
                [
                    pl.col("total_supply")
                    .fill_null(strategy="backward")
                    .over("jj_code", order_by="close_time"),
                    pl.col("market_cap_fdv_ratio")
                    .fill_null(strategy="backward")
                    .over("jj_code", order_by="close_time"),
                ]
            )
        df = df.with_columns(
            [
                pl.col("total_supply")
                .fill_null(strategy="forward")
                .over("jj_code", order_by="close_time"),
                pl.col("market_cap_fdv_ratio")
                .fill_null(strategy="forward")
                .over("jj_code", order_by="close_time"),
            ]
        )
        df = (
            df.filter(pl.col("close_time") >= start)
            .filter(pl.col("close_time") <= end)
            .sort(["jj_code", "close_time"])
        )
        return df


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
    ret = helper.sync(
        "4h",
        workers=1,
        end_time=datetime.now().replace(hour=0).replace(minute=0),
        sync_kline=True,
        sync_info=False,
    )
    print(f"sync ret {ret}")
