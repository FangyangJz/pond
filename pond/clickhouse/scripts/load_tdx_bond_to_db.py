import os
from datetime import datetime
from pathlib import Path

import polars as pl
from tqdm import tqdm

from pond.clickhouse.data_proxy.tdx import TdxDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.enums import Interval, Product, Adjust
from pond.tdx.reader.lc_min_bar_reader import TdxLCMinBarReader

sync_start = datetime(2020, 1, 2)
interval = Interval.MINUTE_5
adjust = Adjust.NFQ
product = Product.BOND

tdx_path = r"E:\new_tdx"  # r"D:\windows\programs\TongDaXin"
host = "192.168.0.103"
password: str = os.environ.get("CLICKHOUSE_PWD", "")

conn_str = f"clickhouse://default:{password}@{host}:18123/quant"
native_conn_str = (
    f"clickhouse+native://default:{password}@{host}:19000/quant?tcp_keepalive=true"
)
manager = ClickHouseManager(conn_str, data_start=sync_start, native_uri=native_conn_str)

data_proxy = TdxDataProxy(tdx_path)
table = data_proxy.get_table(interval=interval, adjust=adjust, product=product)
tdx_reader = TdxLCMinBarReader(interval=interval, tdx_path=Path(tdx_path))
for code in tqdm(data_proxy.get_symobls(product=product)):
    df = tdx_reader.get_df(code=code)
    if df is None:
        continue
    df = df.with_columns(datetime=pl.col("close_time")).to_pandas()
    manager.save_to_db(
        table=table, df=df, datetime_col="open_time", last_record_filters=[table.code == code]
    )
    pass
