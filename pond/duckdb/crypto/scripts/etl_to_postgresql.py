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


def _checkpoint_path(table_name: str) -> Path:
    cur_file = Path(__file__).resolve()
    return cur_file.parent / f".{table_name}.resume"


def read_checkpoint(table_name: str) -> int:
    p = _checkpoint_path(table_name)
    if not p.exists():
        return 0
    try:
        return int(p.read_text().strip() or "0")
    except Exception:
        return 0


def write_checkpoint(table_name: str, offset: int) -> None:
    p = _checkpoint_path(table_name)
    try:
        p.write_text(str(offset))
    except Exception:
        pass


def clear_checkpoint(table_name: str) -> None:
    p = _checkpoint_path(table_name)
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass


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
                open_time TIMESTAMPTZ(6) NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                close_time TIMESTAMPTZ(6) NOT NULL,
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


def ensure_timezone_columns(conn, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_name=%s AND column_name='open_time'",
            (table_name,),
        )
        r1 = cur.fetchone()
        cur.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_name=%s AND column_name='close_time'",
            (table_name,),
        )
        r2 = cur.fetchone()
        if r1 and r1[0] == "timestamp without time zone":
            cur.execute(
                f"ALTER TABLE {table_name} ALTER COLUMN open_time TYPE TIMESTAMPTZ(6) USING open_time AT TIME ZONE 'UTC'"
            )
        if r2 and r2[0] == "timestamp without time zone":
            cur.execute(
                f"ALTER TABLE {table_name} ALTER COLUMN close_time TYPE TIMESTAMPTZ(6) USING close_time AT TIME ZONE 'UTC'"
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
    df = df.with_columns(
        open_time=pl.col("open_time").dt.replace_time_zone("UTC"),
        close_time=pl.col("close_time").dt.replace_time_zone("UTC"),
    )
    if "jj_code" not in df.columns:
        raise RuntimeError("jj_code column is missing")
    df = df.unique(subset=["jj_code", "close_time"], keep="last")
    return df


def df_to_rows(df: pl.DataFrame):
    df = df.select(
        [
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
        ]
    ).sort(["close_time", "jj_code"]) 
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
    env_page_size = int(os.environ.get("POSTGRES_PAGE_SIZE", str(page_size)))
    page_size = max(1000, min(env_page_size, 20000))
    for i in range(0, total, page_size):
        batch = rows[i : i + page_size]
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=page_size)
        conn.commit()
        processed = min(i + page_size, total)
        write_checkpoint(table_name, processed)
        logger.info(
            f"upsert progress: {processed}/{total}, batch={len(batch)}, page_size={page_size}"
        )


@timeit_function_wrapper
def etl_all(interval: str = "1d", asset_type: AssetType = AssetType.spot):
    crypto_path = CryptoPath(Path(r"/home/fangyang/DuckDB/crypto"))

    parquet_dir = crypto_path.get_base_path(asset_type, DataType.klines) / interval
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
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()

    drop_old_table = bool(int(os.environ.get("DROP_OLD_TABLE", "0")))
    if drop_old_table:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

    create_table(conn, table_name)
    ensure_indexes(conn, table_name)
    ensure_timezone_columns(conn, table_name)
    df = pl.read_parquet(list(parquet_dir.glob("*.parquet")))
    if len(df) == 0:
        logger.info("df is empty")
        return

    df = normalize_df(df)
    size_mb = df.estimated_size(unit="mb")
    logger.info(f"df shape: {df.shape}, df size: {size_mb:.2f} MB")

    rows_all = df_to_rows(df)
    page_size_env = int(os.environ.get("POSTGRES_PAGE_SIZE", "5000"))
    while True:
        start_offset = read_checkpoint(table_name)
        try:
            upsert_rows(conn, table_name, rows_all[start_offset:], page_size=page_size_env)
            clear_checkpoint(table_name)
            break
        except psycopg2.OperationalError:
            conn.close()
            logger.warning("connection dropped; reconnect and resume")
            conn = get_pg_conn(
                host=host, port=port, dbname=dbname, user=user, password=password
            )
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
            conn.commit()
            continue
    conn.close()


if __name__ == "__main__":
    for interval in [
        "1h",
        # "1d",
    ]:
        for asset_type in [
            AssetType.spot,
            # AssetType.future_um,
        ]:
            etl_all(interval=interval, asset_type=asset_type)
