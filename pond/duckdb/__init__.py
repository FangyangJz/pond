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
        self.path_stock_info = self.path_stock / 'info'
        self.path_stock_kline_1d = self.path_stock / 'kline_1d'
        self.path_stock_trades = self.path_stock / 'trades'
        self.path_stock_trades_origin = self.path_stock_trades / 'origin'
        self.path_stock_list = [
            self.path_stock, self.path_stock_info, self.path_stock_kline_1d,
            self.path_stock_trades, self.path_stock_trades_origin
        ]

        self.path_bond = db_path / 'bond'
        self.path_bond_info = self.path_bond / 'info'
        self.path_bond_kline_1d = self.path_bond / 'kline_1d'
        self.path_bond_list = [self.path_bond, self.path_bond_info, self.path_bond_kline_1d]

        self.path_list = self.path_stock_list + self.path_bond_list
        self.init_db_path()

    def init_db_path(self):
        [f.mkdir() for f in self.path_list if not f.exists()]

    def update_bond_info(self):
        from pond.akshare.bond import get_bond_basic_df

        start_time = time.perf_counter()
        bond_basic_df, bond_redeem_df = get_bond_basic_df()
        bond_basic_df['上市时间'] = pd.to_datetime(bond_basic_df['上市时间'])
        bond_basic_df['转股起始日'] = pd.to_datetime(bond_basic_df['转股起始日'])
        bond_redeem_df['转股起始日'] = pd.to_datetime(bond_redeem_df['转股起始日'])

        compress = 'ZSTD'

        (self.con.sql('select * from bond_basic_df')
         .write_parquet(str(self.path_bond_info / f'basic.parquet'), compression=compress))
        logger.success(f'Update basic.parquet cost: {time.perf_counter() - start_time}s')

        (self.con.sql('select * from bond_redeem_df')
         .write_parquet(str(self.path_bond_info / f'redeem.parquet'), compression=compress))
        logger.success(f'Update redeem.parquet cost: {time.perf_counter() - start_time}s')

    def update_stock_info(self):
        from pond.akshare.stock import get_all_stocks_df
        from akshare import tool_trade_date_hist_sina

        start_time = time.perf_counter()

        stock_basic_df = get_all_stocks_df()

        calender_df = tool_trade_date_hist_sina().astype('str')
        calender_df['trade_date'] = pd.to_datetime(calender_df['trade_date'])
        calender_df.reset_index(inplace=True)

        compress = 'ZSTD'

        (self.con.sql('select * from stock_basic_df')
         .write_parquet(str(self.path_stock_info / f'basic.parquet'), compression=compress))
        logger.success(f'Update basic.parquet cost: {time.perf_counter() - start_time}s')

        (self.con.sql('select * from calender_df')
         .write_parquet(str(self.path_stock_info / f'calender.parquet'), compression=compress))
        logger.success(f'Update calender.parquet cost: {time.perf_counter() - start_time}s')

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
    db.update_stock_info()
    db.update_bond_info()
    # db.update_stock_trades()
    r1 = db.con.sql(
        rf"SELECT * from read_parquet('{str(db.path_stock_trades / '20230504.parquet')}')")  # order by jj_code, datetime

    r2 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_bond_info / 'basic.parquet')}')")
    r3 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_bond_info / 'redeem.parquet')}')")
    r4 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'basic.parquet')}')")
    r5 = db.con.sql(rf"SELECT * from read_parquet('{str(db.path_stock_info / 'calender.parquet')}')")
    print(1)
