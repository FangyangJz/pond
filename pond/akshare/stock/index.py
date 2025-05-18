from pathlib import Path
import pandas as pd
import akshare as ak


def get_all_index(cache_path: Path = None, update=False):
    cache_file = None
    if cache_path is not None and not update:
        cache_file = cache_path / "all_index.csv"
        if cache_file.exists():
            df = pd.read_csv(cache_file)
            df["index_code"] = df["index_code"].apply(lambda x: f"000000{x}"[-6:])
            return df
    df = ak.index_stock_info()
    if cache_file is not None:
        cache_file.parent.mkdir(exist_ok=True)
        df.to_csv(cache_file)
    return df


def get_index_cons(symbol: str, cache_path: Path = None):
    cache_file = None
    df = None
    if cache_path is not None:
        cache_file = cache_path / "cons" / f"{symbol}.csv"
        if cache_file.exists():
            df = pd.read_csv(cache_file)
    if df is None:
        df = ak.index_stock_cons_sina(symbol)
        if cache_file is not None:
            cache_file.parent.mkdir(exist_ok=True)
            df.to_csv(cache_file)
    df["code"] = df["code"].astype(str).str.zfill(6).unique().tolist()
    return df
