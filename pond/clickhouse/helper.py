from pond.clickhouse.manager import ClickHouseManager
from typing import Optional
from datetime import datetime
from binance.um_futures import UMFutures
from pond.clickhouse.kline import FuturesKline1H
from tqdm import tqdm
import pandas as pd
from pond.utils.times import datetime2utctimestamp_milli, utcstamp_mill2datetime


class FuturesHelper:
    clickhouse: ClickHouseManager = None
    dict_exchange_info = {}
    exchange: UMFutures = None

    def __init__(self, clickhouse: ClickHouseManager) -> None:
        self.clickhouse = clickhouse
        self.exchange = UMFutures()

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

    def sync_futures_kline(self, interval, signal: datetime):
        table = self.get_futures_table(interval)
        if table is None:
            return
        for symbol in tqdm(self.get_perpetual_symbols(signal)):
            code = symbol["pair"]
            lastest_record = self.clickhouse.get_latest_record_time(
                table, table.code == code
            )
            startTime = datetime2utctimestamp_milli(lastest_record)
            endTime = datetime2utctimestamp_milli(signal)
            klines_list = self.exchange.continuous_klines(
                code,
                "PERPETUAL",
                interval,
                startTime=startTime,
                endTime=endTime,
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


if __name__ == "__main__":
    import os
    import datetime as dtm

    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    manager = ClickHouseManager(
        conn_str, data_start=datetime(2023, 1, 1, tzinfo=dtm.timezone.utc)
    )
    helper = FuturesHelper(manager)
    helper.sync_futures_kline("1h", datetime.now(tz=dtm.timezone.utc))
