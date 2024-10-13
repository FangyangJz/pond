from pathlib import Path

import ccxt
import polars as pl
import datetime as dt

from tqdm import tqdm

exchange = ccxt.okx(
    {
        "apiKey": "",
        "secret": "",
        "proxies": {
            "http": "127.0.0.1:7890",  # these proxies won't work for you, they are here for example
            "https": "127.0.0.1:7890",
        },
    }
)

start = int(dt.datetime(2020, 1, 1).timestamp() * 1000)
save_path = Path("./data")
save_path.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    markets = exchange.load_markets()
    swap_dict = {
        k: v
        for k, v in markets.items()
        if v["type"] == "swap" and v["subType"] == "linear"
    }

    df_list = []
    for k, _ in tqdm(swap_dict.items()):
        while True:
            ohlcv = exchange.fetch_ohlcv(
                symbol=k, timeframe="1d", since=start, limit=300
            )
            if len(ohlcv) == 1:
                break

            # TODO
            # https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history
            df = pl.from_records(
                ohlcv,
                orient="row",
                schema={
                    "open_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "count": pl.Float64,
                },
            )
            open_time = df.with_columns((pl.col("open_time") * 1e3).cast(pl.Datetime))[
                "open_time"
            ]
            start = open_time[0]
            end = open_time[-1]
            print(start, end)

            start = df["open_time"][-1]
            df_list.append(df)

        symbol = k.replace("/", "").split(":")[0]
        df = (
            pl.concat(df_list)
            .with_columns(
                (pl.col("open_time") * 1e3).cast(pl.Datetime),
                quote_volume=pl.col("count")
                * (pl.col("close") + pl.col("open") + pl.col("high") + pl.col("low"))
                / 4,
                jj_code=pl.lit(symbol),
            )
            .with_columns(
                close_time=pl.col("open_time").dt.offset_by("23h59m59s"),
            )
            .sort("open_time")
        ).unique(maintain_order=True)

        df.write_parquet(save_path / f"{symbol}.parquet")
