import os
from datetime import datetime
from pathlib import Path

from pond.clickhouse.data_proxy.tdx import TdxDataProxy
from pond.clickhouse.manager import ClickHouseManager
from pond.enums import Interval, Product
from pond.tdx.reader.lc_min_bar_reader import TdxLCMinBarReader

tdx_path = r"E:\new_tdx"  # r"D:\windows\programs\TongDaXin"
host = "192.168.0.103"
password: str = os.environ.get("CLICKHOUSE_PWD", "")
conn_str = f"clickhouse://default:{password}@{host}:18123/quant"
native_conn_str = (
    f"clickhouse+native://default:{password}@{host}:19000/quant?tcp_keepalive=true"
)
sync_start = datetime(2020, 1, 2)
manager = ClickHouseManager(conn_str, data_start=sync_start, native_uri=native_conn_str)
data_proxy = TdxDataProxy(tdx_path)
tdx_reader = TdxLCMinBarReader(interval=Interval.MINUTE_5, tdx_path=Path(tdx_path))
for code in data_proxy.get_symobls(product=Product.BOND):
    df = tdx_reader.get_df(code=code)
    pass
