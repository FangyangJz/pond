import duckdb

from pathlib import Path


class DuckDB:
    def __init__(self, db_path: Path):
        self.con = duckdb.connect()
        self.path = db_path
        self.compress = 'ZSTD'

        self.init_db_path()

    def init_db_path(self):
        raise NotImplementedError


if __name__ == '__main__':
    db = DuckDB(Path(r'D:\DuckDB'))
    print(1)
