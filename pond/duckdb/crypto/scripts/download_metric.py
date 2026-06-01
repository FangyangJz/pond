import os
from pathlib import Path

from pond.duckdb.crypto import CryptoDB, AssetType, DataType

db = CryptoDB(
    # Path(r"/home/fangyang/DuckDB"),
    Path(r"/share/DuckDB/"),
    requests_proxies={
        "host": "127.0.0.1",
        "port": 7890,
        "protocol": "http",
    },
)
db.update_history_data_parallel(
        start="2020-1-1",
        end="2026-05-31",
        asset_type=AssetType.future_um,
        data_type=DataType.metrics,
        timeframe="1d",
        httpx_proxies={"https://": "http://127.0.0.1:7890"},
        skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT", "BTCSTUSDT"],
        do_filter_quote_volume_0=True,
        if_skip_usdc=True,
        ignore_cache=False,
        workers=os.cpu_count() - 2,
    )