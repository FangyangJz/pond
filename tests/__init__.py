# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:01
# @Author   : Fangyang
# @Software : PyCharm


if __name__ == "__main__":
    from pond.duckdb.stock import StockDB
    from pond.duckdb.bond import BondDB
    from pathlib import Path

    db1 = StockDB(Path(r"D:\DuckDB"))
    db2 = BondDB(Path(r"D:\DuckDB"))

    r2 = db2.con.sql(
        rf"SELECT * from read_parquet('{str(db2.path_bond_info / 'basic.parquet')}')"
    )
    r4 = db1.con.sql(
        rf"SELECT * from read_parquet('{str(db1.path_stock_info / 'basic.parquet')}')"
    )
    print(1)
