from pathlib import Path
import os

import polars as pl
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values

from pond.binance_history.type import AssetType, DataType
from pond.duckdb.crypto.path import CryptoPath
from pond.utils.times import timeit_function_wrapper
from pond.utils.file import load_config_dict

contract_type_dict = {
    AssetType.spot: "spot",
    AssetType.future_um: "um_future",
    AssetType.future_cm: "cm_future",
}


def get_pg_conn(
    host: str, port: int, dbname: str, user: str, password: str, timeout: int = 10
):
    uri = os.environ.get("POSTGRES_URI")
    if uri:
        return psycopg2.connect(uri, connect_timeout=timeout)
    if not all([host, dbname, user, password]):
        raise RuntimeError(
            "POSTGRES_URI or POSTGRES_HOST/PORT/DB/USER/PASSWORD must be set"
        )
    ensure_database_exists(host, port, dbname, user, password)
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=timeout,
    )


def ensure_database_exists(host: str, port: int, dbname: str, user: str, password: str):
    timeout = int(os.environ.get("POSTGRES_CONNECT_TIMEOUT", "10"))
    try:
        psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=timeout,
        ).close()
        return
    except Exception:
        pass
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname="postgres",
        user=user,
        password=password,
        connect_timeout=timeout,
    )
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {dbname} WITH ENCODING 'UTF8'")
    conn.close()


def create_table(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                jj_code TEXT NOT NULL,
                open_time TIMESTAMP(6) NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                close_time TIMESTAMP(6) NOT NULL,
                quote_volume DOUBLE PRECISION NOT NULL,
                count BIGINT NOT NULL,
                taker_buy_volume DOUBLE PRECISION NOT NULL,
                taker_buy_quote_volume DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (jj_code, close_time)
            )
            """
        )
    conn.commit()


def ensure_indexes(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code_time ON {table_name} (jj_code, close_time)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_close_time_brin ON {table_name} USING BRIN (close_time)"
        )
    conn.commit()


def normalize_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.schema.get("open_time") == pl.Int64:
        df = df.with_columns(
            open_time=(pl.col("open_time") * 1000).cast(pl.Datetime("us"))
        )
    elif df.schema.get("open_time") != pl.Datetime("us"):
        df = df.with_columns(open_time=pl.col("open_time").cast(pl.Datetime("us")))
    if df.schema.get("close_time") == pl.Int64:
        df = df.with_columns(
            close_time=(pl.col("close_time") * 1000).cast(pl.Datetime("us"))
        )
    elif df.schema.get("close_time") != pl.Datetime("us"):
        df = df.with_columns(close_time=pl.col("close_time").cast(pl.Datetime("us")))
    if "jj_code" not in df.columns:
        raise RuntimeError("jj_code column is missing")
    df = df.unique(subset=["jj_code", "close_time"], keep="last")
    return df


def df_to_rows(df: pl.DataFrame):
    df = df.select([
        "jj_code",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ])
    return list(df.iter_rows(named=False))


def upsert_rows(conn, table_name: str, rows, page_size: int = 5000):
    if not rows:
        return
    sql = f"""
    INSERT INTO {table_name} (
        jj_code, open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume
    ) VALUES %s
    ON CONFLICT (jj_code, close_time) DO UPDATE SET
        open_time = EXCLUDED.open_time,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        quote_volume = EXCLUDED.quote_volume,
        count = EXCLUDED.count,
        taker_buy_volume = EXCLUDED.taker_buy_volume,
        taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume
    """
    total = len(rows)
    if total <= page_size:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=page_size)
    else:
        for i in range(0, total, page_size):
            batch = rows[i : i + page_size]
            with conn.cursor() as cur:
                execute_values(cur, sql, batch, page_size=page_size)
            logger.info(f"upsert progress: {min(i + page_size, total)}/{total}")
    conn.commit()

@timeit_function_wrapper
def etl_all():
    crypto_path = CryptoPath(Path(r"/home/fangyang/DuckDB/crypto"))
    interval = "1h"
    asset_type = AssetType.future_um        
    parquet_dir = (
        crypto_path.get_base_path(asset_type, DataType.klines) / interval
    )
    table_name = f"{contract_type_dict[asset_type]}_kline_{interval}"
    
    # 在当前文件夹下创建 config.toml 文件
    # 内容为：
    # host = "xxx.xxx.xxx.xxx"
    # port = 5432
    # dbname = "binance"
    # user = "xxxxx"
    # password = "xxxxxxx"
    cur_file = Path(__file__).resolve()
    config_dict = load_config_dict(cur_file.parent / "config.toml")
    host = config_dict["host"]
    port = config_dict["port"]
    dbname = config_dict["dbname"]
    user = config_dict["user"]
    password = config_dict["password"]

    conn = get_pg_conn(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    create_table(conn, table_name)
    ensure_indexes(conn, table_name)
    df = pl.read_parquet(list(parquet_dir.glob("*.parquet")))
    if len(df) == 0:
        logger.info("df is empty")
        return

    df = normalize_df(df)
    size_mb = df.estimated_size(unit="mb")
    logger.info(f"df shape: {df.shape}, df size: {size_mb:.2f} MB")

    # page_size=100000 is about 1G memory in cloud
    upsert_rows(conn, table_name, df_to_rows(df), page_size=100000)
    conn.close()


if __name__ == "__main__":
    etl_all()
