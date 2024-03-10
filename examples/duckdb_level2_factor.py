# !/usr/bin/env python3
# @Datetime : 2023/11/17 下午 03:31
# @Author   : Fangyang
# @Software : PyCharm
import time
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from pond.duckdb.stock import StockDB
from tqdm import tqdm


def change_datetime_fields(db: StockDB):
    for dir_path in [
        db.path_stock_level2_trade,
        db.path_stock_level2_order,
        db.path_stock_level2_orderbook,
    ]:
        for file in (pbar := tqdm(dir_path.glob("*.parquet"))):
            pbar.set_postfix_str(str(file))
            rel = db.con.read_parquet(str(file))
            file = file.parent / ("_" + file.name)
            cols = ["自然日 as datetime" if "自然日" == c else c for c in rel.columns]
            rel.select(",".join(cols)).write_parquet(str(file), compression=db.compress)


if __name__ == "__main__":
    db = StockDB(Path(r"E:\DuckDB"))
    # change_datetime_fields(db)

    rel = db.con.read_parquet(str(db.path_stock_level2_trade / "20230508.parquet"))

    start_time = time.perf_counter()
    sh_rel1 = rel.filter("jj_code in ('SHSE.600000')").select("*, price*volume as amt")
    sh_rel1.show()

    sh_rel1_df = sh_rel1.df().set_index("datetime")
    sh_rel1_df_desc = sh_rel1_df.describe()
    sh_rel1_df.hist("amt", bins=200)
    plt.show()
    # sh_rel1_df['amount'] = sh_rel1_df['price'] * sh_rel1_df['volume']
    # sh_rel1_df = sh_rel1_df.between_time("09:30:00", "10:00:00").groupby(['jj_code', 'BS']).sum()

    view1 = (
        sh_rel1.filter(
            "datetime::TIME >= TIME '09:30:00' and datetime::TIME <= TIME '10:00:00'"
        ).aggregate(
            (
                "date_trunc('minute', datetime) as datetime, "
                "first(jj_code) as jj_code, first(BS) as BS, sum(price*volume) as amt"
            ),
            "jj_code, BS, date_trunc('minute', datetime)",
        )
    ).create_view("view1")

    sh_df = view1.df()
    print(f"time cost: {time.perf_counter() - start_time:.2f}s")
    buy_view = (
        view1.filter("BS='B'")
        .select("* EXCLUDE(BS, amt), amt as buy_amt")
        .create_view("buy_view")
    )
    sell_view = (
        view1.filter("BS='S'")
        .select("* EXCLUDE(BS, amt), amt as sell_amt")
        .create_view("sell_view")
    )
    view2 = (
        db.con.sql("select * from buy_view JOIN sell_view USING (jj_code, datetime)")
        .order("jj_code, datetime")
        .create_view("view2")
    )
    view2.show()

    factor_view = (
        view2.select("*, (buy_amt - sell_amt)/(buy_amt + sell_amt) as amt_ratio")
        .aggregate(
            (
                "sum(amt_ratio), mean(amt_ratio), stddev_samp(amt_ratio), kurtosis(amt_ratio),"
                "mean(buy_amt - sell_amt)/stddev_samp(buy_amt - sell_amt) as zscore_net_amt"
            ),
            "jj_code",
        )
        .create_view("factor_view")
    )
    factor_view.show()

    # sh_buy_df = (
    #     sh_df[sh_df['BS'] == 'B'][['jj_code', 'amount', 'datetime']].reset_index(drop=True).rename(
    #         columns={'amount': 'buy_amount'}))
    # sh_sell_df = (
    #     sh_df[sh_df['BS'] == 'S'][['jj_code', 'amount', 'datetime']].reset_index(drop=True).rename(
    #         columns={'amount': 'sell_amount'}))
    # temp_df = sh_buy_df.merge(sh_sell_df, on=['jj_code', 'datetime'], how='outer')
    # temp_df['ratio'] = ((temp_df['buy_amount'] - temp_df['sell_amount'])
    #                     / (temp_df['buy_amount'] + temp_df['sell_amount']))

    # factor_df = temp_df[['jj_code', 'ratio']].groupby('jj_code').mean()
    # factor_df2 = temp_df

    sz_rel = rel.filter("jj_code='SZSE.000571'")
    sz_df = sz_rel.df()
    print(1)
