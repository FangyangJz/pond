#!/usr/bin/env python3
"""
从 PostgreSQL marketcap 表导出数据到 CSV 文件。

配置：复用同目录下的 config.toml（host/port/dbname/user/password）

用法：
    python export_marketcap_csv.py                          # 默认导出 marketcap 表
    python export_marketcap_csv.py --table marketcap_daily   # 指定表名
    python export_marketcap_csv.py --list                    # 列出所有 marketcap 相关表
    python export_marketcap_csv.py --output /path/to.csv     # 指定输出路径
"""

import argparse
import csv
from pathlib import Path

from loguru import logger
import psycopg2

from pond.utils.file import load_config_dict


def get_pg_conn(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )


def list_marketcap_tables(conn) -> list[str]:
    """列出 public schema 下所有带 marketcap 的表"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name LIKE '%market%cap%' "
            "ORDER BY table_name"
        )
        return [r[0] for r in cur.fetchall()]


def get_table_schema(conn, table_name: str) -> list[tuple[str, str]]:
    """返回 [(column_name, data_type), ...]"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s "
            "ORDER BY ordinal_position",
            (table_name,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def export_table_to_csv(conn, table_name: str, output_path: Path):
    """读取整张表并写出 CSV"""
    logger.info(f"Reading table: {table_name}")
    with conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{table_name}" ORDER BY 1')
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    logger.info(f"Fetched {len(rows)} rows, {len(col_names)} columns")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)

    logger.success(f"Exported {len(rows)} rows × {len(col_names)} cols → {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL marketcap table to CSV"
    )
    parser.add_argument("--table", type=str, default="marketcap",
                        help="Table name to export (default: marketcap)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output CSV path (default: scripts/marketcap.csv)")
    parser.add_argument("--list", action="store_true",
                        help="List available marketcap-related tables and exit")
    args = parser.parse_args()

    # ── 加载配置 ──
    cur_file = Path(__file__).resolve()
    config_path = cur_file.parent / "config.toml"
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        return

    config = load_config_dict(config_path)
    conn = get_pg_conn(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )

    # ── --list 模式 ──
    if args.list:
        tables = list_marketcap_tables(conn)
        if tables:
            logger.info("Marketcap-related tables:")
            for t in tables:
                cols = get_table_schema(conn, t)
                col_str = ", ".join(f"{c[0]} ({c[1]})" for c in cols[:8])
                if len(cols) > 8:
                    col_str += f" ... +{len(cols) - 8} more"
                logger.info(f"  {t:<30s} | {col_str}")
        else:
            logger.warning("No marketcap-related tables found")
        conn.close()
        return

    # ── 导出模式 ──
    output_path = Path(args.output) if args.output else cur_file.parent / "marketcap.csv"

    # 检查表是否存在
    tables = list_marketcap_tables(conn)
    table_name = args.table
    if table_name not in tables:
        logger.warning(f"Table '{table_name}' not found in marketcap-related tables")
        logger.info(f"Available tables: {tables}")
        conn.close()
        return

    # 显示 schema
    schema = get_table_schema(conn, table_name)
    logger.info(f"Schema of '{table_name}':")
    for c in schema:
        logger.info(f"  {c[0]:30s} {c[1]}")

    # 导出
    export_table_to_csv(conn, table_name, output_path)
    conn.close()


if __name__ == "__main__":
    main()
