from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
from datetime import datetime
from binance.um_futures import UMFutures
from pond.clickhouse.kline import FuturesKline1H
from tqdm import tqdm
import pandas as pd
from loguru import logger
from pathlib import Path
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
        return None

    def sync_futures_historic_kline(self, pair, interval, start, end):
        pass

    def sync_futures_kline(self, interval) -> bool:
        signal = datetime.now()
        table = self.get_futures_table(interval)
        if table is None:
            return
        data_limit = 720  # 1 month, max 1000
        error_count = 0
        for symbol in tqdm(self.get_perpetual_symbols(signal)):
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            limit_seconds = data_limit * 60 * timeframe2minutes(interval)
            if (signal - lastest_record).total_seconds() > limit_seconds:
                local_klines_df = self.crypto_db.load_history_data(
                    code, lastest_record, signal, timeframe=interval, **self.configs
                )
                self.clickhouse.save_to_db(
                    table, local_klines_df.to_pandas(), table.code == code
                )
                lastest_record = self.clickhouse.get_latest_record_time(
                    table, table.code == code
                )
                if (signal - lastest_record).seconds > limit_seconds:
                    error_count += 1
                    logger.warning(
                        f"futures helper sync kline ignore too long period {lastest_record}-{signal}"
                    )
                    continue

            startTime = datetime2utctimestamp_milli(lastest_record)
            klines_list = self.exchange.continuous_klines(
                code,
                "PERPETUAL",
                interval,
                startTime=startTime,
                limit=1000,
            )
            cols = list(table().get_colcom_names().values())[1:] + ["stub"]
            klines_df = pd.DataFrame(klines_list, columns=cols)
            klines_df["code"] = code
            klines_df["open_time"] = klines_df["open_time"].apply(
                utcstamp_mill2datetime
            )
            klines_df["close_time"] = klines_df["close_time"].apply(
                utcstamp_mill2datetime
            )
            self.clickhouse.save_to_db(table, klines_df, table.code == code)
        return error_count == 0


if __name__ == "__main__":
    import os

    crypto_db = CryptoDB(Path(r"E:\DuckDB"))
    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    manager = ClickHouseManager(conn_str, data_start=datetime(2023, 1, 1))
    helper = FuturesHelper(crypto_db, manager)
    ret = helper.sync_futures_kline("1h")
    print(f"sync ret {ret}")
