import ray
from mootdx.reader import Reader
from pond.tdx.path import tdx_path
import pandas as pd
import os
import math


@ray.remote
class TdxReaderActor:
    def __init__(
        self, cache_path=None, update=False, parquet_compress="ZSTD", fetch_data=True
    ) -> None:
        self.cache_path = cache_path
        self.update = update
        self.compress = parquet_compress
        self.fetch_data = fetch_data
        self.dfs = []

    def get(self):
        if len(self.dfs) > 0 and self.fetch_data:
            return pd.concat(self.dfs)
        return []

    def read(self, tdx_dir, market, symbols, period, start_date=None, end_date=None):
        reader = Reader.factory(market=market, tdxdir=tdx_dir)
        for symbol in symbols:
            if period == 'd':
                df = reader.daily(symbol=symbol)
            else:
                df = reader.minute(symbol=symbol, suffix=period)
            if df is not None and len(df) > 0:
                df = df.reset_index(drop=False)
                df["jj_code"] = symbol
                df["close_time"] = df["date"]
                self.__cache_into_disk__(symbol, df)
                if start_date is not None:
                    df = df[df["date"] >= start_date]
                if end_date is not None:
                    df = df[df["date"] <= end_date]
                self.dfs.append(df)

    def __cache_into_disk__(self, symbol, df: pd.DataFrame):
        if df is None or self.cache_path is None or not self.update:
            return
        df.to_parquet(
            f"{self.cache_path}/{symbol}.parquet", compression=self.compress, index=True
        )


def read_tdx_minute_kline(
    tdx_dir=tdx_path,
    market="std",
    symbols=[],
    period="1",
    start_date=None,
    end_date=None,
    process_counts=(os.cpu_count() - 1),
):
    readers = []
    group_size = math.ceil(len(symbols) / process_counts)

    for i in range(process_counts):
        reader = TdxReaderActor.remote()
        reader.read.remote(
            tdx_dir,
            market,
            symbols[i * group_size : (i + 1) * group_size],
            period,
            start_date,
            end_date,
        )
        readers.append(reader)

    dfs = []
    for reader in readers:
        df = ray.get(reader.get.remote())
        if df is not None and len(df) > 0:
            dfs.append(df)
    return pd.concat(dfs)


if __name__ == "__main__":
    from pond.akshare.stock.all_basic import get_all_stocks_df

    df_stocks = get_all_stocks_df()
    import time
    from datetime import datetime

    # # 这里测试确实cache生效了
    start_time = time.perf_counter()
    df = read_tdx_minute_kline(
        tdx_dir=r"D:\windows\programs\TongDaXin",
        symbols=df_stocks["代码"][:500],
        start_date=datetime(2023, 12, 1),
        end_date=datetime(2024, 1, 26),
        process_counts=1,
    )
    print(f"Time cost:{time.perf_counter()-start_time:.2f}s")
    print(len(df))
