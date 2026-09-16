import os
import datetime as dt

from pond.duckdb.crypto import CryptoDB, AssetType, DataType

# db_path 默认从 pond/duckdb/crypto/.env 的 DB_PATH 读取
db = CryptoDB(
    requests_proxies={
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "http",
    },
)

end_date = dt.datetime.now().strftime("%Y-%m-%d")
db.update_history_data_parallel(
        start="2020-1-1",
        end=end_date,
        asset_type=AssetType.future_um,
        data_type=DataType.metrics,
        timeframe="1d",
        httpx_proxies={"https://": "http://127.0.0.1:7890"},
        skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT", "BTCSTUSDT"],
        do_filter_quote_volume_0=True,
        if_only_usdt=True,
        ignore_cache=False,
        workers=os.cpu_count() - 2,
    )
