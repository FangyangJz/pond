# !/usr/bin/env python3
"""Binance USDT-M (futures/um) aggTrade 月包下载 → 逐月 parquet (ctars ML 计划 Phase 0 数据补全)。

产物布局 (与 pond update_crypto_agg_trades 手动路由命名一致, 审计/因子管线 rglob 兼容):
  /share/DuckDB/crypto/agg_trades/{SYM}-aggTrades-{YYYY-MM}.parquet   (ZSTD)
zip 缓存 (pond binance_history 同构镜像, 供后续增量/手动路由复用):
  /share/DuckDB/crypto/data/futures/um/monthly/aggTrades/{SYM}/{SYM}-aggTrades-{YYYY-MM}.zip

设计 (2026-09, 绕开 pond 两条不可用链路):
  1) pond load_history_data 会把请求区间全部月份 concat 进内存再写盘 — aggTrades 全历史
     数十亿行必然 OOM → 本脚本逐月: 下载 zip → 单月 CSV → 单月 parquet → 释放;
  2) pond update_history_data 对 aggTrades 的增量合并不可用 (time_column 硬编码 close_time,
     aggTrades 无此列 → 有存量文件时整段重下并覆盖) → 月粒度文件天然支持断点续传与增量追加。

用法 (pond venv, pond 仓库根):
  默认 = 协议池 6 标的 (ctars ML 计划 §1.4, v0.2.9): BTC/ETH/BNB/XRP/SOL/AAVE
  试跑: uv run python update_aggtrades_um.py --symbols BTCUSDT --month-limit 2
  XAUUSDT 数据储备 (协议外, 预留给单标的策略): 在 --symbols 中追加 XAUUSDT
  代理策略: 默认先直连, 直连失败自动回退 127.0.0.1:7890 代理;
  --proxy URL 强制只用该代理; --no-proxy 强制只用直连
  已存在 parquet 的月份自动跳过 (断点续传); 强制重下加 --force

后续月度追加 (pond 增量 bug 未修前):
  下载新月 zip → 解压 csv 到 agg_trades/origin/ → CryptoDB.update_crypto_agg_trades()
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import zipfile

import httpx
import polars as pl
from dateutil import parser as dt_parser

from pond.binance_history.type import AssetType, DataType
import pond.duckdb.crypto.const  # 预热: 先完整初始化 crypto 包 (其 __init__ 会 import utils), 断开循环导入
from pond.binance_history.utils import (
    get_local_data_path,
    get_urls_by_xml_parse,
)

DEFAULT_CRYPTO_DIR = Path("/share/DuckDB/crypto")
DEFAULT_AGG_DIR = DEFAULT_CRYPTO_DIR / "agg_trades"
# 协议池 6 标的 (ctars ML 计划 §1.4 v0.2.9); XAUUSDT 为数据储备, 用时追加到 --symbols
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT", "AAVEUSDT"]
DEFAULT_PROXY = "http://127.0.0.1:7890"

# Binance aggTrades CSV 列 → 规范名 (M/is_best_match 不保留, 与 pond get_csv_schema 一致)
LETTER_TO_CANON = {
    "a": "agg_trade_id",
    "p": "price",
    "q": "quantity",
    "f": "first_trade_id",
    "l": "last_trade_id",
    "T": "transact_time",
    "m": "is_buyer_maker",
    "M": "is_best_match",
}
CANON_COLS = ["agg_trade_id", "price", "quantity", "first_trade_id",
              "last_trade_id", "transact_time", "is_buyer_maker"]


def _read_agg_csv(zip_path: Path, stem: str) -> pl.DataFrame:
    """读 zip 内单月 CSV → 规范列 parquet 前的 DataFrame (处理有/无表头两种情况)。"""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        assert len(csv_names) == 1, f"unexpected csv count in {zip_path}: {csv_names}"
        raw = zf.read(csv_names[0])

    first_line = raw.splitlines()[0].decode("utf-8-sig").strip()
    first_field = first_line.split(",")[0]
    has_header = first_field in LETTER_TO_CANON  # 字母表头 → 有表头
    full_name_header = first_field in CANON_COLS  # 全名表头 (新格式, 如 XAUUSDT 月包)
    df = pl.read_csv(raw, has_header=(has_header or full_name_header), infer_schema_length=10000)

    if has_header:
        df = df.rename({c: LETTER_TO_CANON.get(c, c) for c in df.columns})
    elif full_name_header:
        pass  # 列名已是规范名
    else:  # 无表头: 按 Binance 顺序逐列命名
        n = len(df.columns)
        cols = ["agg_trade_id", "price", "quantity", "first_trade_id",
                "last_trade_id", "transact_time", "is_buyer_maker", "is_best_match"]
        assert n <= len(cols), f"unexpected column count {n} in {zip_path}"
        df.columns = cols[:n]

    return df.select(CANON_COLS).with_columns(
        pl.col("agg_trade_id").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Float64),
        pl.col("first_trade_id").cast(pl.Int64),
        pl.col("last_trade_id").cast(pl.Int64),
        pl.col("transact_time").cast(pl.Int64),
        pl.col("is_buyer_maker").cast(pl.Boolean),
    )


def _month_of(url: str) -> str:
    m = re.search(r"(\d{4}-\d{2}(?:-\d{2})?)\.zip$", url)
    return m.group(1) if m else "unknown"


def main() -> None:
    ap = argparse.ArgumentParser(description="download Binance UM aggTrades monthly -> parquet")
    ap.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    ap.add_argument("--start", default="2020-01-01", help="含 onboardDate 截断 (月粒度)")
    ap.add_argument("--end", default="2026-10-01", help="远端存在的月才下载, 空月自动跳过")
    ap.add_argument("--crypto-dir", type=Path, default=DEFAULT_CRYPTO_DIR)
    ap.add_argument("--agg-dir", type=Path, default=DEFAULT_AGG_DIR)
    ap.add_argument("--proxy", default=None,
                    help=f"强制只用该代理 (默认 {DEFAULT_PROXY}); 缺省时先直连、失败自动回退代理")
    ap.add_argument("--no-proxy", action="store_true", help="强制只用直连, 失败也不回退代理")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--month-limit", type=int, default=0, help="每标的只处理前 N 个月 (试跑)")
    args = ap.parse_args()

    start_dt = dt_parser.parse(args.start)
    end_dt = dt_parser.parse(args.end)
    # 代理策略: 默认 [直连] 优先, 网络失败自动回退 [代理]; --proxy 强制代理; --no-proxy 强制直连
    direct_px: dict[str, str] = {}
    proxy_px: dict[str, str] = {
        "http://": args.proxy or DEFAULT_PROXY,
        "https://": args.proxy or DEFAULT_PROXY,
    }
    if args.no_proxy:
        plans = [direct_px]
    elif args.proxy:
        plans = [proxy_px]
    else:
        plans = [direct_px, proxy_px]
    args.agg_dir.mkdir(parents=True, exist_ok=True)

    def _run_with_fallback(fn):
        last_err: Exception | None = None
        for px in plans:
            try:
                return fn(px)
            except Exception as e:  # noqa: BLE001 — 网络模式切换后重试
                last_err = e
                tag = "direct" if not px else f"proxy {px['https://']}"
                print(f"  [net] {type(e).__name__}: {e} -> retry via {tag}")
        assert last_err is not None
        raise last_err

    def _fetch_zip(px, url: str, zip_path: Path) -> None:
        client = httpx.Client(proxies=px, trust_env=False, timeout=None, follow_redirects=True)
        try:
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = zip_path.with_suffix(".zip.part")
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
            tmp.rename(zip_path)
        finally:
            client.close()

    for sym in args.symbols:
        load_urls, download_urls = _run_with_fallback(
            lambda px: get_urls_by_xml_parse(
                data_type=DataType.aggTrades,
                asset_type=AssetType.future_um,
                symbol=sym,
                start=start_dt,
                end=end_dt,
                timeframe="1M",
                file_path=args.crypto_dir,
                proxies=px,
            )
        )
        urls = sorted(load_urls, key=_month_of)
        if args.month_limit:
            urls = urls[: args.month_limit]
        print(f"[{sym}] months to process: {len(urls)} (first={_month_of(urls[0]) if urls else '-'})")

        n_ok = n_skip = n_fail = 0
        for url in urls:
            month = _month_of(url)
            zip_path = get_local_data_path(url, args.crypto_dir)
            out_parquet = args.agg_dir / f"{zip_path.stem}.parquet"
            if out_parquet.exists() and not args.force:
                n_skip += 1
                continue
            try:
                if url in download_urls and not zip_path.exists():
                    _run_with_fallback(lambda px: _fetch_zip(px, url, zip_path))
                df = _read_agg_csv(zip_path, zip_path.stem)
                t_min = df["transact_time"].min()
                t_max = df["transact_time"].max()
                first_m = datetime.utcfromtimestamp(t_min / 1000).strftime("%Y-%m")
                last_m = datetime.utcfromtimestamp(t_max / 1000).strftime("%Y-%m")
                df.write_parquet(out_parquet, compression="zstd")
                print(f"  [{sym} {month}] ok rows={df.shape[0]:,} "
                      f"span={first_m}..{last_m} -> {out_parquet.name} "
                      f"({out_parquet.stat().st_size / 1e9:.2f} GB)")
                del df
                n_ok += 1
            except Exception as e:  # noqa: BLE001 — 单月失败不中断
                print(f"  [{sym} {month}] FAILED: {type(e).__name__}: {e}")
                n_fail += 1
        print(f"[{sym}] done: ok={n_ok} skip={n_skip} fail={n_fail}")
    print(f"\noutput dir: {args.agg_dir}")


if __name__ == "__main__":
    main()
