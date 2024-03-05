import akshare as ak
from tqdm import tqdm
from pathlib import Path
import polars as pl

def update_industry_list(path:Path):
    df_industry_list = ak.stock_board_industry_name_ths()
    df_industry_list.to_csv(path / f"industry_list.csv")

def update_industry_constituent(path:Path):
    df_industry_list = read_industry_list(path).collect().to_pandas()
    for industry in tqdm(df_industry_list.itertuples()):
        cache_dir = path / "cons"
        if not cache_dir.exists():
            cache_dir.mkdir()
        cache_file = cache_dir / f"{industry.code}_{industry.name}.csv"
        df = ak.stock_board_industry_cons_ths(symbol=industry.name)
        if df is not None and len(df) >= 3:
            df.to_csv(cache_file)

def read_industry_list(path:Path) -> pl.LazyFrame:
    return pl.scan_csv(path / "industry_list.csv")


def read_industry_consituent(path:Path) -> pl.LazyFrame:
    dfs = []
    for file in (path / "cons").glob("*.csv"):
        df = pl.scan_csv(file).with_columns([
            pl.col("代码").apply(lambda x : f"00000{x}"[-6:]).alias("code"),
            pl.lit(file.stem).alias("industry")
        ]).select(["industry", "code"])
        dfs.append(df)
    return pl.concat(dfs)