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

    def get_spot_pairs(self):
        spot_info = self.exchange.exchange_info()  # 调用现货exchange_info接口
        spot_pairs = set()
        for symbol in spot_info["symbols"]:
            # 条件1：交易对状态为"TRADING"（可交易）
            if symbol["status"] != "TRADING":
                continue
            has_spot_permission = any(
                "SPOT" in perm_set for perm_set in symbol.get("permissionSets", [])
            )
            if has_spot_permission:
                base = symbol["baseAsset"]
                quote = symbol["quoteAsset"]
                spot_pairs.add((base, quote))
        return spot_pairs

    def get_valid_um_futures(self):
        """通过UMFutures类筛选有对应现货的永续合约"""
        spot_pairs = self.get_spot_pairs()
        if not spot_pairs:
            return []

        futures_info = self.um_future_exchange.exchange_info()

        # 3. 筛选有对应现货的合约
        valid_futures = []
        for symbol in futures_info["symbols"]:
            if symbol["status"] == "TRADING":
                # 处理合约符号（去除永续合约的"PERP"后缀）
                symbol_clean = symbol["symbol"].replace("PERP", "")
                base_asset = None
                quote_asset = "USDT"
                if symbol_clean.endswith(quote_asset):
                    base_asset = symbol_clean[: -len(quote_asset)]

                # 检查是否在现货组合中
                if (
                    base_asset
                    and quote_asset
                    and (base_asset, quote_asset) in spot_pairs
                ):
                    valid_futures.append(
                        {
                            "symbol": symbol["symbol"],
                            "base_asset": base_asset,
                            "quote_asset": quote_asset,
                            "contract_type": symbol.get("contractType"),
                            "margin_asset": symbol.get("marginAsset"),
                        }
                    )

        return valid_futures

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
    data_proxy: DirectDataProxy = None
    fix_kline_with_cryptodb: bool = None
    gecko_id_mapper: CoinGeckoIDMapper = CoinGeckoIDMapper()
    verbose_log = False

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
        symbols = self.data_proxy.get_valid_um_futures()
        return symbols

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
        threads = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            worker = Thread(
                target=self.__sync_kline,
                args=(signal, table, worker_symbols, interval, res_dict),
            )
            worker.start()
            threads.append(worker)

        [t.join() for t in threads]
        if len(res_dict) != len(threads):
            logger.error(
                "res dict length not equal to threads length, child thread may exited unexpectedly."
            )
            return False
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
            latest_kline_df.group_by("close_time").len().sort("close_time")[-1, "len"]
        )
        if request_count <= lastest_count:
            logger.info(
                f"spot helper sync kline for {interval} finished, request count: {request_count}, latest count: {lastest_count}"
            )
            return True
        return False

    def __sync_kline(
        self,
        signal,
        table: SpotKline1H,
        symbols,
        interval,
        res_dict: dict,
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = 0
        interval_seconds = timeframe2minutes(interval) * 60
        data_limit = 720  # 1 month, max 1000
        limit_seconds = data_limit * timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        kline_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        kline_df = pl.from_pandas(kline_df)
        for symbol in symbols:
            code = symbol["symbol"]
            latest_record = kline_df.filter(pl.col("code") == code)
            lastest_record = (
                latest_record[0, "datetime"]
                if len(latest_record) > 0
                else self.clickhouse.data_start
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
                if self.verbose_log:
                    logger.debug(
                        f"spot helper sync kline ignore too short duration {lastest_record}-{signal} for {code}"
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
            except ClientError:
                continue
            except Exception as e:
                logger.error(f"spot helper sync kline for {code} failed {e}")
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
            .dt.cast_time_unit(df["close_time"].dtype.time_unit)
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
        existence_df = existence_df.select(
            ["close_time", "jj_code", "spot_existence", "spot_volume"]
        )
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
