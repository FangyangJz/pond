import os
from datetime import datetime

from pond.clickhouse.manager import ClickHouseManager
from pond.clickhouse.stock_helper import StockHelper
from pond.enums import Interval, Adjust, Product
from pond.clickhouse.data_proxy.akshare import AKShareFundNetValueProxy

password: str = os.environ.get("CLICKHOUSE_PWD", "")
conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
native_conn_str = (
    f"clickhouse+native://default:{password}@localhost:9000/quant?tcp_keepalive=true"
)
sync_start = datetime(2015, 1, 2)
manager = ClickHouseManager(conn_str, data_start=sync_start, native_uri=native_conn_str)
proxy = AKShareFundNetValueProxy()
proxy.min_sync_interval_days = 5
helper = StockHelper(manager)
helper.data_proxy = proxy
ret = False
while not ret:
    ret = helper.sync_kline(
        interval=Interval.DAY_1, adjust=Adjust.HFQ, product=Product.FUND, workers=1
    )
