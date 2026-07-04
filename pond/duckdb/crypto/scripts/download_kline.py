import os
import datetime as dt
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

end_date = dt.datetime.now().strftime("%Y-%m-%d")
for asset_type in [AssetType.future_um, AssetType.spot]:
    for timeframe in ["1h", "1d"]:
        db.update_history_data_parallel(
            start="2020-1-1",
            end=end_date,
            asset_type=asset_type,
            data_type=DataType.klines,
            timeframe=timeframe,
            httpx_proxies={"https://": "http://127.0.0.1:7890"},
            skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT", "BTCSTUSDT"],
            do_filter_quote_volume_0=True,
            if_only_usdt=True,
            ignore_cache=False,
            workers=os.cpu_count() - 2,
        )
