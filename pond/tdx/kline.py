import ray
from mootdx.reader import Reader
from pond.tdx.path import tdx_path
import pandas as pd


@ray.remote
def read_tdx_minute_kline_distrubuted(**kwargs):
    market = kwargs["market"]  # std
    tdx_dir = kwargs["tdx_dir"]
    symbol = kwargs["symbol"]
    period = kwargs["period"]
    reader = Reader.factory(market=market, tdxdir=tdx_dir)
    df = reader.minute(symbol=symbol, suffix=period)
    if df is not None:
        df = df.reset_index(drop=False)
        if 'start_date' in kwargs.keys():
            df = df[df["date"] >= kwargs['start_date']]
        if 'end_date' in kwargs.keys():
            df = df[df["date"] <= kwargs['end_date']]
    return  df

def read_tdx_minute_kline(
    tdx_dir=tdx_path,
    market="std",
    symbols=[],
    period="1",
    start_date=None,
    end_date=None,
):
    kwargs = {}
    kwargs["tdx_dir"] = tdx_dir
    kwargs["market"] = market
    kwargs["period"] = period
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    futures = []
    for symbol in symbols:
        kwargs["symbol"] = symbol
        futures.append(read_tdx_minute_kline_distrubuted.remote(**kwargs))
    dfs = ray.get(futures)
    dfs = [df for df in dfs if df is not None and len(df) > 0]
    return pd.concat(dfs)


if __name__ == "__main__":
    from pond.akshare.stock.all_basic import get_all_stocks_df

    df_stocks = get_all_stocks_df()
    import time
    from datetime import datetime

    # # 这里测试确实cache生效了
    start_time = time.perf_counter()
    df = read_tdx_minute_kline(tdx_dir=r"D:\windows\programs\TongDaXin", symbols=df_stocks['代码'][:500], start_date=datetime(2023,12,1), end_date=datetime(2024,1,26))
    print(f"Time cost:{time.perf_counter()-start_time:.2f}s")
    print(len(df))
