#!/usr/bin/env python3
"""下载指定永续合约的 1m K 线到本地 DuckDB 目录。"""

import argparse
import datetime as dt
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import polars as pl
from loguru import logger

from pond.duckdb.crypto import AssetType, CryptoDB, DataType


DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XAUUSDT"]
DEFAULT_DB_PATH = Path("/share/DuckDB")
DEFAULT_PROXY_HOST = "127.0.0.1"
DEFAULT_PROXY_PORT = 7890
DEFAULT_PROXY_PROTOCOL = "http"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="下载指定 future_um 永续合约的 1m kline 数据。"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="要下载的 futures symbols，默认: BTCUSDT ETHUSDT XAUUSDT",
    )
    parser.add_argument(
        "--start",
        default="2020-01-01",
        help="起始日期，格式如 2020-01-01",
    )
    parser.add_argument(
        "--end",
        default=dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"),
        help="结束日期，格式如 2026-07-18",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="DuckDB 根目录，默认 /share/DuckDB",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="并发 worker 数；只下载 3 个标的时一般不需要手动指定",
    )
    parser.add_argument(
        "--ignore-cache",
        action="store_true",
        help="忽略已有 parquet，强制重下时间区间内数据",
    )
    parser.add_argument(
        "--disable-proxy",
        action="store_true",
        help="禁用默认代理",
    )
    parser.add_argument(
        "--proxy-host",
        default=DEFAULT_PROXY_HOST,
        help="代理 host，默认 127.0.0.1",
    )
    parser.add_argument(
        "--proxy-port",
        type=int,
        default=DEFAULT_PROXY_PORT,
        help="代理 port，默认 7890",
    )
    parser.add_argument(
        "--proxy-protocol",
        default=DEFAULT_PROXY_PROTOCOL,
        help="代理协议，默认 http",
    )
    return parser.parse_args()


def normalize_symbols(symbols: list[str]) -> list[str]:
    return sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})


def build_proxy_args(args: argparse.Namespace) -> tuple[dict | None, dict[str, str]]:
    if args.disable_proxy:
        return None, {}

    requests_proxy = {
        "host": args.proxy_host,
        "port": args.proxy_port,
        "protocol": args.proxy_protocol,
    }
    proxy_url = f"{args.proxy_protocol}://{args.proxy_host}:{args.proxy_port}"
    httpx_proxy = {"http://": proxy_url, "https://": proxy_url}
    return requests_proxy, httpx_proxy


def build_target_asset_info(db: CryptoDB, symbols: list[str]) -> pd.DataFrame:
    df = db.get_future_info(AssetType.future_um, from_local=False)
    df = df[df["contract_type"].isin(["PERPETUAL", "TRADIFI_PERPETUAL"])][
        ["symbol", "contract_type", "deliveryDate", "onboardDate", "update_datetime"]
    ]

    manual_file = Path(__file__).resolve().parent.parent / "UMFutures_manual.csv"
    if manual_file.exists():
        df = pd.concat([df, pd.read_csv(manual_file)], ignore_index=True)

    df = df.sort_values(by="symbol").drop_duplicates(subset=["symbol"])
    return df[df["symbol"].isin(symbols)].copy()


def log_saved_parquet_summary(db: CryptoDB, symbols: list[str]) -> None:
    storage_path = db.crypto_path.kline_um / "1m"
    for symbol in symbols:
        parquet_file = storage_path / f"{symbol}.parquet"
        if not parquet_file.exists():
            logger.warning(f"[{symbol}] parquet not found: {parquet_file}")
            continue

        summary = pl.read_parquet(
            parquet_file,
            columns=["open_time", "close_time"],
        )
        if summary.is_empty():
            logger.warning(f"[{symbol}] parquet is empty: {parquet_file}")
            continue

        logger.success(
            f"[{symbol}] saved to {parquet_file}, rows={summary.height}, "
            f"range={summary['open_time'].min()} -> {summary['close_time'].max()}"
        )


def download_selected_symbols(
    db: CryptoDB,
    asset_info_df: pd.DataFrame,
    start: str,
    end: str,
    httpx_proxy: dict[str, str],
    ignore_cache: bool,
    workers: int,
) -> None:
    workers = max(1, min(workers, len(asset_info_df)))
    if workers == 1:
        db.update_history_data(
            asset_info_df=asset_info_df,
            start=start,
            end=end,
            asset_type=AssetType.future_um,
            data_type=DataType.klines,
            timeframe="1m",
            httpx_proxies=httpx_proxy,
            skip_symbols=[],
            do_filter_quote_volume_0=False,
            if_only_usdt=False,
            ignore_cache=ignore_cache,
            worker_id=0,
        )
        return

    chunk_size = (len(asset_info_df) + workers - 1) // workers
    chunks = [
        asset_info_df.iloc[idx : idx + chunk_size].copy()
        for idx in range(0, len(asset_info_df), chunk_size)
    ]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                db.update_history_data,
                chunk,
                start,
                end,
                AssetType.future_um,
                DataType.klines,
                "1m",
                httpx_proxy,
                [],
                False,
                False,
                ignore_cache,
                worker_id,
            )
            for worker_id, chunk in enumerate(chunks)
        ]

        for future in futures:
            future.result()


def main() -> int:
    args = parse_args()
    symbols = normalize_symbols(args.symbols)
    if not symbols:
        logger.error("symbols 不能为空")
        return 1

    requests_proxy, httpx_proxy = build_proxy_args(args)
    db = CryptoDB(args.db_path, requests_proxies=requests_proxy)

    asset_info_df = build_target_asset_info(db, symbols)
    found_symbols = asset_info_df["symbol"].tolist()
    missing_symbols = sorted(set(symbols) - set(found_symbols))

    logger.info(
        f"准备下载 future_um 1m klines, symbols={symbols}, start={args.start}, end={args.end}"
    )
    if missing_symbols:
        logger.warning(f"以下 symbols 未在 futures info 中找到，将跳过: {missing_symbols}")
    if asset_info_df.empty:
        logger.error("没有可下载的 symbol，退出")
        return 1

    workers = args.workers
    if workers is None:
        workers = max(1, min(len(asset_info_df), (os.cpu_count() or 1) - 2))

    download_selected_symbols(
        db=db,
        asset_info_df=asset_info_df,
        start=args.start,
        end=args.end,
        httpx_proxy=httpx_proxy,
        ignore_cache=args.ignore_cache,
        workers=workers,
    )

    logger.info(f"下载完成，workers={workers}")
    log_saved_parquet_summary(db, found_symbols)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
