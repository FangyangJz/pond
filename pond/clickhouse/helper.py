import os
import math
from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
import datetime as dtm
from datetime import datetime
import time
import polars as pl
from binance.um_futures import UMFutures
from pond.clickhouse.kline import (
    FutureInfo,
    FutureFundingRate,
    FuturesKline5m,
    FuturesKline4H,
    FuturesKline1H,
    FuturesKline1d,
    FuturesKline15m,
    TokenHolders,
    FutureOpenInterest,
    FutureLongShortRatio,
    FutureLongShortPositionRatio,
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
from pond.coin_gecko.contract_info import BinanceContractTool
from pond.chain_base.client import ChainbaseClient
from pond.chain_base import ChainId
from pond.binance_history.futures_api import (
    get_long_short_account_ratio_history,
    get_long_short_position_ratio_history,
    get_open_interest_history,
)
from binance.client import Client
from pond.binance_history.websocket_client import BinanceWSClientWrapper


class DataProxy:
    def um_future_exchange_info(self) -> dict:
        pass

    def um_future_klines(
        self, symbol, contract_type, interval, startTime, limit
    ) -> list:
        pass

    def um_future_funding_rate(self, symbol, contract_type, interval, startTime, limit):
        pass


class DirectDataProxy(DataProxy):
    exchange: UMFutures = None
    # 添加缓存相关变量
    _exchange_info_cache = None
    _exchange_info_cache_time = None

    def __init__(self) -> None:
        self.exchange = UMFutures(
            proxies={"https": "127.0.0.1:7890", "http": "127.0.0.1:7890"}
        )

    def um_future_exchange_info(self) -> dict:
        # 检查缓存是否存在且未过期（24小时）
        current_time = time.time()
        if (
            self._exchange_info_cache is not None
            and self._exchange_info_cache_time is not None
        ):
            if (
                current_time - self._exchange_info_cache_time < 24 * 60 * 60
            ):  # 24小时有效期
                logger.info(
                    f"get exchanged info from cache within 24 hours, cache time is: {self._exchange_info_cache_time}"
                )
                return self._exchange_info_cache

        # 缓存不存在或已过期，重新获取数据
        exchange_info = self.exchange.exchange_info()
        # 更新缓存和时间戳
        self._exchange_info_cache = exchange_info
        self._exchange_info_cache_time = current_time
        return exchange_info

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

    def um_future_funding_rate(self, symbol, contract_type, interval, startTime, limit):
        return self.exchange.funding_rate(symbol, startTime=startTime, limit=limit)


class FuturesHelper:
    crypto_db: CryptoDB = None
    clickhouse: ClickHouseManager = None
    dict_exchange_info = {}
    exchange: UMFutures = None
    data_proxy: DataProxy = None
    fix_kline_with_cryptodb: bool = None
    gecko_id_mapper: CoinGeckoIDMapper = CoinGeckoIDMapper()
    contact_tool: BinanceContractTool = BinanceContractTool()
    binance: Client = None
    binance_wss: dict[str, BinanceWSClientWrapper] = {}
    verbose_log = False
    proxy_host = None
    proxy_port = None

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
        api_key = kwargs.get(
            "CHAIN_BASE_API_KEY", os.environ.get("CHAIN_BASE_API_KEY", None)
        )
        self.chainbase_client = ChainbaseClient(api_key)
        binance_api_key = kwargs.get(
            "BINANCE_API_KEY", os.environ.get("BINANCE_API_KEY", None)
        )
        binance_api_secret = kwargs.get(
            "BINANCE_API_SECRET", os.environ.get("BINANCE_API_SECRET", None)
        )
        self.binance = Client(binance_api_key, binance_api_secret)
        self.proxy_host = kwargs.get("proxy_host", None)
        self.proxy_port = kwargs.get("proxy_port", None)

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
        try:
            exchange_info_dict = self.get_exchange_info(signal)
            return [
                symbol
                for symbol in exchange_info_dict["symbols"]
                if symbol["contractType"] == "PERPETUAL"
                and symbol["pair"].endswith("USDT")
                and symbol["status"] == "TRADING"
            ]
        except Exception as e:
            logger.error(e)
            return None

    def get_futures_table(self, interval, data) -> Optional[FuturesKline1H]:
        if data == "kline":
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
        elif data == "info":
            if interval == "1d":
                return FutureInfo
        elif data == "funding_rate":
            if interval == "1h":
                return FutureFundingRate
        elif data == "holders":
            if interval == "1d":
                return TokenHolders
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
        ret = self.sync(interval, workers, end_time, what="kline")
        return ret

    def sync_futures_info(
        self, interval, workers=None, end_time: datetime = None
    ) -> bool:
        ret = self.sync(interval, workers, end_time, what="info")
        return ret

    def sync(
        self,
        interval,
        workers=None,
        end_time: datetime = None,
        what=None,
        allow_missing_count=0,
    ) -> bool:
        table = self.get_futures_table(interval, what)
        if end_time is not None:
            signal = end_time
        else:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        if workers is None:
            workers = math.ceil(os.cpu_count() / 2)
        symbols = self.get_perpetual_symbols(signal)
        if symbols is None:
            return False
        task_counts = math.ceil(len(symbols) / workers)
        res_dict = {}
        threads = []
        for i in range(0, workers):
            worker_symbols = symbols[i * task_counts : (i + 1) * task_counts]
            if what == "kline":
                worker = Thread(
                    target=self.__sync_futures_kline,
                    args=(signal, table, worker_symbols, interval, res_dict),
                )
                worker.start()
                threads.append(worker)
            elif what == "info":
                worker2 = Thread(
                    target=self.__sync_futures_info,
                    args=(signal, table, worker_symbols, res_dict),
                )
                worker2.start()
                threads.append(worker2)
            elif what == "funding_rate":
                worker3 = Thread(
                    target=self.__sync_futures_funding_rate,
                    args=(signal, table, worker_symbols, interval, res_dict),
                )
                worker3.start()
                threads.append(worker3)
            elif what == "holders":
                worker4 = Thread(
                    target=self.__sync_futures_base_asset_holders,
                    args=(table, worker_symbols, interval, res_dict),
                )
                worker4.start()
                threads.append(worker4)
            else:
                worker5 = Thread(
                    target=self.__sync_futures_extra_info,
                    args=(what, signal, worker_symbols, interval, res_dict),
                )
                worker5.start()
                threads.append(worker5)

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
        if what in ["kline", "funding_rate"]:
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
            time_col = "close_time" if what == "kline" else "fundingTime"
            lastest_count = (
                latest_kline_df.group_by(time_col).len().sort(time_col)[-1, "len"]
            )
            # allow 1 target data missing.
            if lastest_count < request_count - allow_missing_count:
                return False
        return True

    def save_klines_from_ws(self, interval):
        client = self.binance_wss.get(interval, None)
        if client is None:
            logger.debug("futures helper save_klines_from_ws ignored, client is None")
            return
        klines_df = client.get_aggregated_kline_dataframe()
        client.clear_all_data()
        if klines_df is None or len(klines_df) == 0:
            logger.debug(
                "futures helper save_klines_from_ws ignored, klines_df is None or empty"
            )
            return
        # check kline completed.
        klines_df = (
            klines_df.sort(["pair", "open_time"])
            .with_columns(
                open_time_1=pl.col("open_time")
                .shift(1)
                .over("pair", order_by="open_time")
            )
            .with_columns(open_time_gap=pl.col("open_time") - pl.col("open_time_1"))
        )
        uncomplited_klines_df = klines_df.filter(
            pl.col("open_time_gap") > pl.duration(minutes=timeframe2minutes(interval))
        )
        klines_df = klines_df.filter(
            ~pl.col("pair").is_in(uncomplited_klines_df["pair"])
        )
        # check kline start time.
        min_open_time_df: pl.DataFrame = (
            klines_df.group_by("pair")
            .agg(pl.col("open_time").min().alias("min_open_time"))
            .rename({"pair": "code"})
        )
        table = self.get_futures_table(interval, "kline")
        latest_klines_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, datetime.now() - timedelta(days=30), datetime.now(), 1
        )
        latest_klines_df = pl.from_pandas(
            latest_klines_df[["code", "datetime"]]
        ).with_columns(datetime=pl.col("datetime").dt.replace_time_zone("UTC"))
        min_open_time_df = min_open_time_df.join(
            latest_klines_df, on="code", how="left"
        )
        min_open_time_df = min_open_time_df.filter(
            pl.col("min_open_time") - pl.col("datetime") <= pl.duration(seconds=1)
        )
        klines_df = klines_df.filter(pl.col("pair").is_in(min_open_time_df["code"]))
        klines_df = klines_df.to_pandas()
        klines_df["code"] = klines_df["pair"]
        klines_df["datetime"] = klines_df["close_time"]
        logger.debug(f"futures helper save_klines_from_ws data len {len(klines_df)}")
        self.clickhouse.save_dataframe(table.__tablename__, klines_df)

    def __sync_futures_kline(
        self, signal, table: FuturesKline1H, symbols, interval, res_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        data_limit = 720  # 1 month, max 1000
        limit_seconds = data_limit * timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        latest_klines_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        latest_klines_df = pl.from_pandas(latest_klines_df)
        for symbol in symbols:
            code = symbol["pair"]
            latest_record = latest_klines_df.filter(pl.col("code") == code)
            lastest_record = (
                latest_record[0, "datetime"]
                if len(latest_record) > 0
                else self.clickhouse.data_start
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
                if self.verbose_log:
                    logger.debug(
                        f"futures helper sync kline ignore too short duration {lastest_record}-{signal}"
                    )
                continue

            startTime = datetime2utctimestamp_milli(lastest_record)
            try:
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
            except Exception as e:
                logger.error(f"futures helper sync kline failed {e}")
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

    def __sync_futures_funding_rate(
        self, signal, table: FutureFundingRate, symbols, interval, res_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        for symbol in symbols:
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()

            if data_duration_seconds < interval_seconds:
                if self.verbose_log:
                    logger.debug(
                        f"futures helper sync funding rate ignore too short duration {lastest_record}-{signal} for {code}"
                    )
                continue
            logger.info(
                f"futures helper sync funding rate for {code}, lastest record is {lastest_record}, signal {signal}"
            )
            startTime = datetime2utctimestamp_milli(lastest_record)
            try:
                funding_list = self.data_proxy.um_future_funding_rate(
                    code,
                    "PERPETUAL",
                    interval,
                    startTime=startTime,
                    limit=1000,
                )
                if not funding_list or len(funding_list) == 0:
                    continue
            except Exception as e:
                logger.error(f"futures helper sync funding rate for {code} failed {e}")
                continue
            cols = list(table().get_colcom_names().values())
            funding_rate_df = pd.DataFrame(funding_list, columns=cols)
            funding_rate_df["fundingRate"] = funding_rate_df["fundingRate"].astype(
                "Float64"
            )
            funding_rate_df["markPrice"] = pd.to_numeric(funding_rate_df["markPrice"])
            funding_rate_df["fundingTime"] = funding_rate_df["fundingTime"].apply(
                utcstamp_mill2datetime
            )
            funding_rate_df = funding_rate_df.drop_duplicates(subset=["fundingTime"])
            self.clickhouse.save_to_db(table, funding_rate_df, table.code == code)
        res_dict[tid] = True

    def __sync_futures_base_asset_holders(
        self, table: TokenHolders, symbols, interval, res_dict: dict
    ):
        table = TokenHolders
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        holders_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        holders_df = pl.from_pandas(holders_df)
        synced_count = 0
        for symbol in symbols:
            code = symbol["pair"]
            base_asset = symbol["baseAsset"]
            holder_info = holders_df.filter(pl.col("code") == code)
            lastest_record = (
                holder_info[0, "datetime"] if len(holder_info) > 0 else None
            )
            if (
                lastest_record is not None
                and lastest_record + timedelta(seconds=interval_seconds) > signal
            ):
                if self.verbose_log:
                    logger.debug(
                        f"futures helper token holders ignore too short duration {lastest_record}-{signal} for {code}"
                    )
                synced_count += 1
                continue
            try:
                cg_id = self.gecko_id_mapper.get_coingecko_id(
                    base_asset, exact_match=True
                )
                if cg_id is None:
                    logger.warning(
                        f"futures helper sync holders query coin gecko id for {code} failed"
                    )
                    continue
                if cg_id == "":
                    if self.verbose_log:
                        logger.info(
                            f"futures helper sync holders can not find coin gecko id for {code}"
                        )
                    synced_count += 1
                    continue
                chain_info = self.contact_tool.get_token_chain_info(cg_id)
                holders_dfs = []
                if chain_info is None:
                    continue
                if len(chain_info) == 0:
                    synced_count += 1
                    continue
                for chain, address in chain_info.items():
                    if address is None or len(address.strip()) == 0:
                        continue
                    chain_id = ChainId.get_chain_id(chain)
                    if chain_id is None:
                        continue
                    holders = self.chainbase_client.get_topn_holders(
                        chain_id, address, page=1, limit=20
                    )
                    time.sleep(1)
                    df = pd.DataFrame(data=holders)
                    df["chain"] = chain
                    df["code"] = code
                    df["datetime"] = signal
                    holders_dfs.append(df)
            except Exception as e:
                logger.error(f"futures helper sync holders for {code} failed {e}")
                continue
            if len(holders_dfs) > 0:
                result_df = pd.concat(holders_dfs)
                self.clickhouse.save_to_db(
                    table, result_df, table.code == code, drop_duplicates=False
                )
            synced_count += 1
        res_dict[tid] = synced_count == len(symbols)

    def __sync_futures_info(self, signal, table: FutureInfo, symbols, res_dict: dict):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        info_df = self.clickhouse.read_latest_n_record(
            table.__tablename__, signal - timedelta(days=30), signal, 1
        )
        info_df = pl.from_pandas(info_df)
        count = 0
        for symbol in symbols:
            code = symbol["pair"]
            latest_record = info_df.filter(pl.col("code") == code)
            lastest_record = (
                latest_record[0, "datetime"]
                if len(latest_record) > 0
                else self.clickhouse.data_start
            )
            if signal - lastest_record < timedelta(days=1):
                count += 1
                if self.verbose_log:
                    logger.info(
                        f"futures helper sync info ignore too short duration for {code}"
                    )
                continue
            query = code[:-4]
            cg_id = self.gecko_id_mapper.get_coingecko_id(query, exact_match=True)
            if cg_id is None:
                logger.warning(
                    f"futures helper sync info query coin gecko id for {code} failed"
                )
                continue
            if cg_id == "":
                logger.warning(
                    f"futures helper sync info can not find coin gecko id for {code}"
                )
                self.clickhouse.save_to_db(
                    table,
                    pd.DataFrame(
                        {
                            "datetime": [signal],
                            "code": [code],
                            "total_supply": None,
                            "market_cap_fdv_ratio": None,
                        }
                    ),
                    table.code == code,
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

    def __sync_futures_funding_rate(
        self, signal, table: FutureFundingRate, symbols, interval, res_dict: dict
    ):
        tid = threading.current_thread().ident
        res_dict[tid] = False
        interval_seconds = timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        for symbol in symbols:
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()

            if data_duration_seconds < interval_seconds:
                if self.verbose_log:
                    logger.debug(
                        f"futures helper sync funding rate ignore too short duration {lastest_record}-{signal} for {code}"
                    )
                continue
            startTime = datetime2utctimestamp_milli(lastest_record)
            try:
                funding_list = self.data_proxy.um_future_funding_rate(
                    code,
                    "PERPETUAL",
                    interval,
                    startTime=startTime,
                    limit=1000,
                )
                if not funding_list or len(funding_list) == 0:
                    continue
            except Exception as e:
                logger.error(f"futures helper sync funding rate for {code} failed {e}")
                continue
            cols = list(table().get_colcom_names().values())
            funding_rate_df = pd.DataFrame(funding_list, columns=cols)
            funding_rate_df["fundingRate"] = funding_rate_df["fundingRate"].astype(
                "Float64"
            )
            funding_rate_df["markPrice"] = pd.to_numeric(funding_rate_df["markPrice"])
            funding_rate_df["fundingTime"] = funding_rate_df["fundingTime"].apply(
                utcstamp_mill2datetime
            )
            funding_rate_df = funding_rate_df.drop_duplicates(subset=["fundingTime"])
            self.clickhouse.save_to_db(table, funding_rate_df, table.code == code)
        res_dict[tid] = True

    def __sync_futures_extra_info(
        self, data_name, signal, symbols, interval, res_dict: dict
    ):
        tid = threading.current_thread().ident
        failure_count = 0
        interval_seconds = timeframe2minutes(interval) * 60
        if signal is None:
            signal = datetime.now(tz=dtm.timezone.utc).replace(tzinfo=None)
        if data_name == "long_short_ratio":
            table = FutureLongShortRatio
        elif data_name == "open_interest":
            table = FutureOpenInterest
        elif data_name == "long_short_position_ratio":
            table = FutureLongShortPositionRatio

        for symbol in symbols:
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            data_duration_seconds = (signal - lastest_record).total_seconds()

            if data_duration_seconds < interval_seconds:
                if self.verbose_log:
                    logger.debug(
                        f"futures helper sync funding rate ignore too short duration {lastest_record}-{signal} for {code}"
                    )
                continue
            logger.info(
                f"futures helper __sync_futures_extra_info {data_name} for {code}, lastest record is {lastest_record}, signal {signal}"
            )
            if data_name == "long_short_ratio":
                df = get_long_short_account_ratio_history(
                    self.binance, code, interval, lastest_record, signal
                )
            elif data_name == "long_short_position_ratio":
                df = get_long_short_position_ratio_history(
                    self.binance, code, interval, lastest_record, signal
                )
            elif data_name == "open_interest":
                df = get_open_interest_history(
                    self.binance, code, interval, lastest_record, signal
                )
            if df is None:
                failure_count += 1
                continue
            if len(df) == 0:
                continue
            self.clickhouse.save_to_db(table, df, table.code == code)
            time.sleep(0.01)
        res_dict[tid] = failure_count == 0

    def subscribe_futures(
        self,
        interval,
    ):
        binance_ws = self.binance_wss.get(interval, None)
        if binance_ws is None:
            symbols = self.get_perpetual_symbols(datetime.now())
            symbols = [s["pair"] for s in symbols]
            binance_ws = BinanceWSClientWrapper(
                symbols=symbols,
                interval=interval,
                proxy_host=self.proxy_host,
                proxy_port=self.proxy_port,
                market_type="futures",
            )
            binance_ws.start_all()
            self.binance_wss[interval] = binance_ws
            logger.debug(
                f"futures helper subscribe_futures {interval} total symbols {len(symbols)}"
            )

    def unsubscribe_futures(self, interval):
        binance_ws = self.binance_wss.get(interval, None)
        if binance_ws is not None:
            binance_ws.stop_all()
            self.binance_wss[interval] = None
            logger.debug(f"futures helper unsubscribe_futures {interval}")

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

    def attach_futures_holders(self, df: pl.DataFrame):
        start = df["close_time"].min() - timedelta(days=1)
        end = df["close_time"].max() + timedelta(days=1)
        df = df.with_columns(close_time=pl.col("close_time").dt.cast_time_unit("ns"))
        if "date" not in df.columns:
            df = df.with_columns(date=pl.col("close_time").dt.date())
        holders_df = self.clickhouse.native_read_table(
            TokenHolders, start, end, filters=None, rename=True
        )
        holders_df = pl.from_pandas(holders_df)
        holders_df = holders_df.with_columns(
            date=pl.col("close_time").dt.date(),
        )
        holders_df = holders_df.group_by(["jj_code", "date"]).agg(
            top20_amount=pl.col("amount").sum(),
            chain=pl.col("chain").unique().str.join(","),
            top20_address=(pl.col("chain") + "-" + pl.col("wallet_address"))
            .unique()
            .str.join(","),
            usd_value=pl.col("usd_value").sum(),
        )
        df = df.join(
            holders_df,
            on=["jj_code", "date"],
            how="left",
        )
        return df

    def attach_future_extra_data(self, df: pl.DataFrame):
        start = df["close_time"].min()
        end = df["close_time"].max()
        df = df.with_columns(close_time=pl.col("close_time").dt.cast_time_unit("ns"))
        open_interest_df = self.clickhouse.native_read_table(
            FutureOpenInterest, start, end, filters=None, rename=True
        )
        open_interest_df = pl.from_pandas(open_interest_df)
        long_short_ratio_df = self.clickhouse.native_read_table(
            FutureLongShortRatio, start, end, filters=None, rename=True
        )
        long_short_ratio_df = pl.from_pandas(long_short_ratio_df)
        df = df.join(
            open_interest_df,
            on=["jj_code", "close_time"],
            how="left",
        )
        df = df.join(
            long_short_ratio_df,
            on=["jj_code", "close_time"],
            how="left",
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
    helper = FuturesHelper(
        crypto_db,
        manager,
        proxy_host="127.0.0.1",
        proxy_port=7890,
    )
    interval = "15m"
    end_time = datetime.now().replace(minute=0).replace(second=0).replace(microsecond=0)
    ret = False
    while not ret:
        ret = helper.sync(
            interval,
            workers=1,
            end_time=end_time,
            what="kline",
        )
        print(f"sync ret {ret}")

    helper.subscribe_futures(interval)
    time.sleep(60)
    # helper.unsubscribe_futures("1m")
    while True:
        helper.save_klines_from_ws(interval)
        time.sleep(10)
