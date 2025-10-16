import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
import datetime as dtm
from datetime import datetime
from binance.spot import Spot
from binance.um_futures import UMFutures
from pond.clickhouse.kline import (
    SpotKline1H,
)
from pond.binance_history.type import AssetType
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
from pond.coin_gecko.id_mapper import CoinGeckoIDMapper
from binance.error import ClientError
import polars as pl

# TODO 2.修复没有现货的合约k线下载失败导致数据量判断错误，sync接口一致返回false问题
# TODO 3.修改主项目预测服务，通过调整分组实现没有现货的合约不做空


class DataProxy:
    def exchange_info(self) -> dict:
        pass

    def klines(self, symbol, contract_type, interval, startTime, limit) -> list:
        pass


class DirectDataProxy(DataProxy):
    um_future_exchange = None
    exchange: Spot = None

    def __init__(self) -> None:
        self.um_future_exchange = UMFutures(
            proxies={"https": "127.0.0.1:7890", "http": "127.0.0.1:7890"}
        )
        self.exchange = Spot(
            proxies={"https": "127.0.0.1:7890", "http": "127.0.0.1:7890"}
        )

    def exchange_info(self) -> dict:
        return self.um_future_exchange.exchange_info()

    def klines(self, symbol, contract_type, interval, startTime, limit) -> list:
        return self.exchange.klines(
            symbol,
            interval,
            startTime=startTime,
            limit=1000,
        )


class SpotHelper:
    crypto_db: CryptoDB = None
    clickhouse: ClickHouseManager = None
    dict_exchange_info = {}
    exchange: Spot = None
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
        dict_exchange_info = self.data_proxy.exchange_info()
        self.dict_exchange_info = {key: dict_exchange_info}
        return self.dict_exchange_info[key]

    def get_symbols(self, signal: datetime):
        exchange_info_dict = self.get_exchange_info(signal)
        return [
            symbol
            for symbol in exchange_info_dict["symbols"]
            if symbol["contractType"] == "PERPETUAL"
            and symbol["pair"].endswith("USDT")
            and symbol["status"] == "TRADING"
        ]

    def get_table(self, interval) -> Optional[SpotKline1H]:
        if interval == "1h":
            return SpotKline1H
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

    def sync(
        self,
        interval,
        workers=None,
        end_time: datetime = None,
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
        symbols = self.get_symbols(signal)
        task_counts = math.ceil(len(symbols) / workers)
        res_dict = {}
        ignored_dict = {}
        threads = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = Thread(
                target=self.__sync_kline,
                args=(signal, table, worker_symbols, interval, res_dict, ignored_dict),
            )
            worker.start()
            threads.append(worker)

        [t.join() for t in threads]

        for tid in res_dict.keys():
            if not res_dict[tid]:
                # return false if any thread failed.
                return False
        ignored_count_total = sum(ignored_dict.values())
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
        if request_count <= lastest_count + ignored_count_total:
            return True
        return False

    def __sync_kline(
        self, signal, table, symbols, interval, res_dict: dict, ignored_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = 0
        ignored_count = 0
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
                    code,
                    lastest_record,
                    signal,
                    timeframe=interval,
                    asset_type=AssetType.spot,
                    **self.configs,
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
            try:
                klines_list = self.data_proxy.klines(
                    code,
                    "PERPETUAL",
                    interval,
                    startTime=startTime,
                    limit=1000,
                )
                if not klines_list:
                    # generate stub kline to mark latest sync time.
                    klines_list = [self.gen_stub_kline_as_list(lastest_record, signal)]
            except ClientError as ce:
                if ce.error_code == -1121:
                    logger.info(f"futures helper ignored for invalid symbol {code}")
                    ignored_count += 1
                    continue
            except Exception as e:
                logger.error(f"futures helper sync kline for {code} failed {e}")
                continue
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
        ignored_dict[tid] = ignored_count

    def attach_spot_existence(self, df: pl.DataFrame, extra_query_days=30):
        start = df["close_time"].min()
        end = df["close_time"].max()

        spot_kline_df = self.clickhouse.native_read_table(
            SpotKline1H, start - timedelta(days=extra_query_days), end, rename=True
        )
        spot_kline_df = pl.from_pandas(spot_kline_df)
        spot_kline_df = spot_kline_df.with_columns(
            close_time=(pl.col("close_time") + pl.duration(seconds=1))
            .dt.round(every="1m")
            .dt.cast_time_unit("ms")
        ).rename(
            mapping={
                col: f"spot_{col}"
                for col in spot_kline_df.columns
                if col not in ["close_time", "jj_code"]
            }
        )
        existence_df = df.join(spot_kline_df, on=["close_time", "jj_code"], how="full")
        existence_df = existence_df.sort(["jj_code", "close_time"]).with_columns(
            pl.col("spot_volume").fill_null(strategy="forward").over("jj_code")
        )
        existence_df = existence_df.with_columns(
            pl.col("spot_volume").is_not_null().alias("spot_existence")
        ).filter(pl.col("close_time") >= start)
        df = df.join(existence_df, on=["close_time", "jj_code"], how="left")
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
    helper = SpotHelper(crypto_db, manager)
    helper.fix_kline_with_cryptodb = False
    ret = False
    while not ret:
        ret = helper.sync(
            "1h",
            workers=3,
            end_time=datetime.now().replace(hour=0).replace(minute=0),
        )
    print(f"sync ret {ret}")
