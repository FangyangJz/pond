import duckdb
from pathlib import Path

from duckdb import DuckDBPyRelation
from pond.duckdb.type import df_types, DataFrameStrType, DataFrameType


class DuckDB:
    def __init__(self, db_path: Path, df_type: DataFrameStrType):
        self.con = duckdb.connect()
        self.path = db_path
        self.df_type = df_type
        self.compress = "ZSTD"

        self.init_db_path()

    def init_db_path(self):
        raise NotImplementedError

    def transform_to_df(self, rel: DuckDBPyRelation) -> DataFrameType:
        if self.df_type == df_types.pandas:
            return rel.df()
        elif self.df_type == df_types.polars:
            return rel.pl()
        elif self.df_type == df_types.arrow:
            return rel.arrow()
        else:
            raise TypeError(f"df_type='{self.df_type}' not in {DataFrameStrType}")


if __name__ == "__main__":
    from pond.duckdb.stock import StockDB

    pl_db = StockDB(Path(r"E:\DuckDB"), df_type=df_types.polars)
    pd_db = StockDB(Path(r"E:\DuckDB"), df_type=df_types.pandas)
    pa_db = StockDB(Path(r"E:\DuckDB"), df_type=df_types.arrow)
    df1 = pl_db.get_kline_1d_qfq_df()
    df2 = pd_db.get_kline_1d_qfq_df()
    df3 = pa_db.get_kline_1d_qfq_df()

    err_db = StockDB(Path(r"E:\DuckDB"), df_type="unknow")
    err_db.transform_to_df("dd")
    print(1)
