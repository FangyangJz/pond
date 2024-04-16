import akshare as ak
from tqdm import tqdm
from pathlib import Path
import polars as pl


def update_industry_list(path: Path):
    df_industry_list = ak.stock_board_industry_name_ths()
    df_industry_list.to_csv(path / "industry_list.csv")


def update_industry_constituent(path: Path):
    df_industry_list = read_industry_list(path).collect().to_pandas()
    for industry in tqdm(df_industry_list.itertuples()):
        cache_dir = path / "cons"
        if not cache_dir.exists():
            cache_dir.mkdir()
        cache_file = cache_dir / f"{industry.code}_{industry.name}.csv"
        df = ak.stock_board_industry_cons_ths(symbol=industry.name)
        if df is not None and len(df) >= 3:
            df.to_csv(cache_file)


def read_industry_list(path: Path) -> pl.LazyFrame:
    return pl.scan_csv(path / "industry_list.csv")


def read_industry_consituent(path: Path) -> pl.LazyFrame:
    dfs = []
    for file in (path / "cons").glob("*.csv"):
        df = (
            pl.scan_csv(file)
            .with_columns(
                [
                    pl.col("代码").apply(lambda x: f"00000{x}"[-6:]).alias("code"),
                    pl.lit(file.stem).alias("industry"),
                ]
            )
            .select(["industry", "code"])
        )
        dfs.append(df)
    return pl.concat(dfs)


def gen_industry_index(df_snapshot: pl.LazyFrame, path: Path, compress="zstd"):
    """
    编制行业分时指数,生成:
    --------------------
    industry_chgpct 行业涨跌幅
    industry_volume 行业成交量
    industry_amount 行业成交额
    industry_pn_ratio 行业涨跌比例
    industry_found 行业流入流出资金
    industry_size 行业成分股数目
    """
    cache_dir = path / "index"
    if not cache_dir.exists():
        cache_dir.mkdir()
    df_industry_cons = read_industry_consituent(path).collect()
    df_industries = df_industry_cons["industry"].unique()
    for industry in tqdm(df_industries, desc="Generating industry index..."):
        df_industry = df_industry_cons.filter(pl.col("industry") == industry)
        df_industry_snapshot = (
            df_snapshot.join(df_industry.lazy(), on="code")
            .with_columns(
                [
                    pl.when(pl.col("dchgpct") > 0)
                    .then(1)
                    .when(pl.col("dchgpct") < 0)
                    .then(-1)
                    .otherwise(0)
                    .alias("pn_count")
                ]
            )
            .group_by("datetime")
            .agg(
                [
                    pl.col("dchgpct").sum().alias("industry_chgpct"),
                    pl.col("volume").sum().alias("industry_volume"),
                    pl.col("amount").sum().alias("industry_amount"),
                    pl.col("found").sum().alias("industry_found"),
                    (pl.col("pn_count").sum() / len(df_industry)).alias(
                        "industry_pn_ratio"
                    ),
                ]
            )
            .with_columns(
                [
                    pl.lit(len(df_industry)).alias("industry_size"),
                    pl.lit(industry).alias("industry"),
                ]
            )
            .sort("datetime")
        )
        df_industry_snapshot.collect().write_parquet(
            cache_dir / f"{industry}.parquet", compression=compress
        )


def read_industry_index(path: Path, industry=None) -> pl.LazyFrame:
    if industry is not None:
        return pl.scan_parquet(path / "index" / f"{industry}.parquet")
    return pl.scan_parquet(path / "index" / "*.parquet")
