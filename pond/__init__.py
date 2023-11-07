import time
import duckdb
import pandas as pd
from loguru import logger

from pathlib import Path


class DuckDB:
    def __init__(self, db_path: Path):
        self.con = duckdb.connect()
        self.path = db_path

        self.path_stock = db_path / 'stock'
        self.path_stock_kline_1d = self.path_stock / 'kline_1d'
        self.path_stock_trades = self.path_stock / 'trades'
        self.path_stock_trades_origin = self.path_stock_trades / 'origin'
        self.path_stock_list = [
            self.path_stock, self.path_stock_kline_1d, self.path_stock_trades, self.path_stock_trades_origin
        ]

        self.path_bond = db_path / 'bond'
        self.path_bond_kline_1d = self.path_bond / 'kline_1d'
        self.path_bond_list = [self.path_bond, self.path_bond_kline_1d]

        self.path_list = self.path_stock_list + self.path_bond_list
        self.init_db_path()

    def init_db_path(self):
        [f.mkdir() for f in self.path_list if not f.exists()]

    def update_stock_trades(self):
        from pond.stock.trades import get_trade_df_with_multiprocess

        for dir_path in self.path_stock_trades_origin.glob('*'):
            if dir_path.is_dir():
                date_str = dir_path.stem
                df2 = get_trade_df_with_multiprocess(dir_path)

                compress = 'ZSTD'
                start_time = time.perf_counter()
                (self.con.sql('select * from df2')
                 .write_parquet(str(self.path_stock_trades / f'{date_str}.parquet'), compression=compress))
                logger.success(f'Update {date_str}.parquet cost: {time.perf_counter() - start_time}s')


if __name__ == '__main__':
    db = DuckDB(Path(r'D:\DuckDB'))
    # db.update_stock_trades()
    rr = db.con.sql(
        rf"SELECT * from read_parquet('D:\DuckDB\stock\trades\20230504.parquet')")  # order by jj_code, datetime
    print(1)