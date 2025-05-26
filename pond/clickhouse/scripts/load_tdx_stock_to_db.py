import os
from datetime import datetime

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.stock_helper import StockHelper
from pond.enums import Interval, Adjust

tdx_path = r"E:\new_tdx"  # r"D:\windows\programs\TongDaXin"
password: str = os.environ.get("CLICKHOUSE_PWD", "")
conn_str = f"clickhouse://default:{password}@localhost:18123/quant"
native_conn_str = (
    f"clickhouse+native://default:{password}@localhost:19000/quant?tcp_keepalive=true"
)
sync_start = datetime(2020, 1, 2)
manager = ClickHouseManager(conn_str, data_start=sync_start, native_uri=native_conn_str)
helper = StockHelper(manager, tdx_path=tdx_path)
helper.sync_kline(
    interval=Interval.MINUTE_5, adjust=Adjust.NFQ
)
