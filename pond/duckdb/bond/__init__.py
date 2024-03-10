# !/usr/bin/env python3
# @Datetime : 2023/11/8 1:33
# @Author   : Fangyang
# @Software : PyCharm

import time
from pathlib import Path

import pandas as pd
from loguru import logger

from pond.duckdb import DuckDB, DataFrameStrType, df_types


class BondDB(DuckDB):
    def __init__(self, db_path: Path, df_type: DataFrameStrType = df_types.pandas):
        self.path_bond = db_path / "bond"
        self.path_bond_info = self.path_bond / "info"
        self.path_bond_kline_1d = self.path_bond / "kline_1d"
        self.path_bond_list = [
            self.path_bond,
            self.path_bond_info,
            self.path_bond_kline_1d,
        ]

        super().__init__(db_path, df_type)

    def init_db_path(self):
        [f.mkdir() for f in self.path_bond_list if not f.exists()]

    @property
    def bond_basic_df(self):
        return self.con.sql(
            rf"SELECT * from read_parquet('{str(self.path_bond_info / 'basic.parquet')}')"
        ).df()

    @property
    def bond_redeem_df(self):
        return self.con.sql(
            rf"SELECT * from read_parquet('{str(self.path_bond_info / 'redeem.parquet')}')"
        ).df()

    def kline_1d_df(self):
        return self.con.sql(
            rf"SELECT * from read_parquet({[str(f) for f in self.path_bond_kline_1d.iterdir()]})"
        ).df()

    def update_bond_info(self):
        from pond.akshare.bond import get_bond_basic_df

        start_time = time.perf_counter()
        bond_basic_df, bond_redeem_df = get_bond_basic_df()
        bond_basic_df["上市时间"] = pd.to_datetime(bond_basic_df["上市时间"])
        bond_basic_df["转股起始日"] = pd.to_datetime(bond_basic_df["转股起始日"])
        bond_redeem_df["转股起始日"] = pd.to_datetime(bond_redeem_df["转股起始日"])

        (
            self.con.sql("select * from bond_basic_df").write_parquet(
                str(self.path_bond_info / f"basic.parquet"), compression=self.compress
            )
        )
        logger.success(
            f"Update basic.parquet cost: {time.perf_counter() - start_time}s"
        )

        (
            self.con.sql("select * from bond_redeem_df").write_parquet(
                str(self.path_bond_info / f"redeem.parquet"), compression=self.compress
            )
        )
        logger.success(
            f"Update redeem.parquet cost: {time.perf_counter() - start_time}s"
        )

    def update_bond_kline_1d(self):
        from pond.duckdb.bond.kline import get_bond_daily_df_by_reader

        start_time = time.perf_counter()
        k1d_df = get_bond_daily_df_by_reader(self.bond_basic_df)
        filename = f'{k1d_df["trade_date"].min().strftime("%Y%m%d")}_{k1d_df["trade_date"].max().strftime("%Y%m%d")}'

        for f in self.path_bond_kline_1d.iterdir():
            if f.stem == filename:
                logger.info(
                    f"{filename}.parquet has been created, not save memory df to disk."
                )
                return
            else:
                # clear existing file
                f.unlink()

        logger.info("Start to write parquet file ...")
        (
            self.con.sql("select * from k1d_df").write_parquet(
                str(self.path_bond_kline_1d / f"{filename}.parquet"),
                compression=self.compress,
            )
        )

        logger.success(
            f"Update all parquet file cost: {time.perf_counter() - start_time:.2f}s"
        )


if __name__ == "__main__":
    db = BondDB(Path(r"E:\DuckDB"))

    db.update_bond_info()
    db.update_bond_kline_1d()

    dd = db.kline_1d_df()
    r2 = db.con.sql(
        rf"SELECT * from read_parquet('{str(db.path_bond_info / 'basic.parquet')}')"
    )
    r3 = db.con.sql(
        rf"SELECT * from read_parquet('{str(db.path_bond_info / 'redeem.parquet')}')"
    )
    print(1)
