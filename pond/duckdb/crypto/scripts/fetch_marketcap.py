#!/usr/bin/env python3
"""
从 CoinGecko /coins/markets 拉取全市场市值数据，写入 parquet 日快照。

数据包含：当前价格、市值、市值排名、成交量、24h/7d/30d 涨跌幅、
总供应量、最大供应量等基础面指标。

存储结构：
    {db_path}/crypto/marketcap/daily/{YYYY-MM-DD}.parquet       — 全量 CoinGecko 数据
    {db_path}/crypto/marketcap/binance/{YYYY-MM-DD}.parquet     — 只保留 binance 永续合约标的

消歧策略：
    ticker 在 CoinGecko 上可能对应多个币（如 BTC → bitcoin, batcat, ...），
    脚本从 /coins/markets 拉取已按市值排好序的数据，每个 ticker 只保留市值最高的那一个。

用法：
    python fetch_marketcap.py                    # 拉取今天
    python fetch_marketcap.py --force             # 强制重拉今天
    python fetch_marketcap.py --date 2026-01-01   # 拉取指定日期
    python fetch_marketcap.py --db-path /share/DuckDB
"""

import argparse
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from tqdm import tqdm

from pond.binance_history.type import AssetType
from pond.duckdb.crypto import CryptoDB
from pond.duckdb.crypto.path import CryptoPath


COINGECKO_API = "https://api.coingecko.com/api/v3"
REQUEST_INTERVAL = 2.5  # 免费 API ~10-30 次/分钟
PER_PAGE = 250
MAX_RETRIES = 5


def _session(proxies: dict | None = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "pond/1.0"})
    if proxies:
        s.proxies.update(proxies)
    return s


def _fetch_with_retry(url: str, params: dict, proxies: dict | None = None) -> list[dict]:
    """带指数退避的请求封装，处理 429 限频"""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = _session(proxies).get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = min(2**attempt * 5, 120)
            logger.warning(f"429 rate limited (attempt {attempt}/{MAX_RETRIES}), "
                           f"waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Max retries ({MAX_RETRIES}) exceeded for {url}")


def fetch_markets_page(page: int, proxies: dict | None = None) -> list[dict]:
    """GET /coins/markets 单页，按市值降序排列"""
    return _fetch_with_retry(
        f"{COINGECKO_API}/coins/markets",
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": PER_PAGE,
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "1h,24h,7d,30d",
        },
        proxies,
    )


def fetch_all_markets(proxies: dict | None = None) -> pl.DataFrame:
    """遍历所有分页，返回完整市场数据的 polars DataFrame"""
    all_data = []
    page = 1
    pbar = tqdm(desc="Fetching CoinGecko markets")
    while True:
        data = fetch_markets_page(page, proxies)
        if not data:
            break
        all_data.extend(data)
        pbar.update(1)
        page += 1
        time.sleep(REQUEST_INTERVAL)
    pbar.close()

    logger.info(f"Fetched {len(all_data)} coins from CoinGecko ({page - 1} pages)")

    now = datetime.now(timezone.utc)
    records = []
    for c in all_data:
        records.append(
            {
                "coingecko_id": c.get("id"),
                "symbol": (c.get("symbol") or "").upper(),
                "name": c.get("name"),
                "current_price": c.get("current_price"),
                "market_cap": c.get("market_cap"),
                "market_cap_rank": c.get("market_cap_rank"),
                "fully_diluted_valuation": c.get("fully_diluted_valuation"),
                "total_volume": c.get("total_volume"),
                "high_24h": c.get("high_24h"),
                "low_24h": c.get("low_24h"),
                "price_change_24h": c.get("price_change_24h"),
                "price_change_pct_24h": c.get("price_change_percentage_24h"),
                "price_change_pct_7d": c.get(
                    "price_change_percentage_7d_in_currency"
                ),
                "price_change_pct_30d": c.get(
                    "price_change_percentage_30d_in_currency"
                ),
                "circulating_supply": c.get("circulating_supply"),
                "total_supply": c.get("total_supply"),
                "max_supply": c.get("max_supply"),
                "ath": c.get("ath"),
                "ath_date": c.get("ath_date"),
                "atl": c.get("atl"),
                "atl_date": c.get("atl_date"),
                "fetch_time": now,
            }
        )
    return pl.DataFrame(records)


def build_ticker_map_from_markets(df_markets: pl.DataFrame) -> dict[str, str]:
    """
    从已按市值排序的市场数据构建 ticker → coingecko_id 映射。

    关键：/coins/markets 已按 market_cap 降序排列，只需要每个 ticker 取
    第一个出现的 coin（市值最高），自然消歧。
    """
    ticker_map: dict[str, str] = {}
    for row in df_markets.iter_rows(named=True):
        ticker = (row["symbol"] or "").lower()
        if ticker and ticker not in ticker_map:
            ticker_map[ticker] = row["coingecko_id"]
    return ticker_map


def strip_quote(symbol: str) -> str:
    """
    去除报价货币，提取基础资产名。
    BTCUSDT → BTC
    1000PEPEUSDT → PEPE (去除 1000/100/1MIL 前缀)
    """
    for q in (
        "USDT",
        "USDC",
        "BUSD",
        "FDUSD",
        "TUSD",
        "DAI",
        "USD",
        "EUR",
        "BNB",
        "BTC",
        "ETH",
    ):
        if symbol.endswith(q) and len(symbol) > len(q):
            base = symbol[: -len(q)]
            if base.startswith("1000"):
                return base[4:]
            if base.startswith("100"):
                return base[3:]
            if base.startswith("1MIL"):
                return base[4:]
            return base
    return symbol


def strip_leveraged(symbol: str) -> str:
    """处理杠杆/做空代币：BTCDOWNUSDT → BTC, ETHUPUSDT → ETH"""
    for suffix in ("UP", "DOWN", "BULL", "BEAR", "HEDGE"):
        if symbol.endswith(suffix + "USDT") and len(symbol) > len(suffix + "USDT"):
            return symbol[: -len(suffix + "USDT")]
    return symbol


def resolve_binance_to_coingecko(
    binance_symbols: list[str],
    ticker_map: dict[str, str],
) -> dict[str, str]:
    """
    Binance 永续合约 symbol → CoinGecko ID 映射。

    消歧逻辑：ticker_map 已保证每个 ticker 只保留市值最高的那一个，
    所以这里直接查表即可。
    """
    result: dict[str, str] = {}
    for sym in binance_symbols:
        base = strip_quote(sym).lower()
        if not base:
            base = strip_leveraged(sym).lower()
        cg_id = ticker_map.get(base)
        if not cg_id:
            logger.warning(f"No CoinGecko match for {sym} (base={base})")
            continue
        result[sym] = cg_id
    return result


def register_marketcap_paths(crypto_path: CryptoPath):
    """确保 marketcap 目录存在"""
    for p in [
        crypto_path.crypto / "marketcap",
        crypto_path.crypto / "marketcap" / "daily",
        crypto_path.crypto / "marketcap" / "binance",
    ]:
        p.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch CoinGecko market cap snapshot"
    )
    parser.add_argument("--force", action="store_true", help="Force re-fetch today")
    parser.add_argument(
        "--date", type=str, default=None, help="Date tag (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/share/DuckDB",
        help="DuckDB base path (default: /share/DuckDB)",
    )
    args = parser.parse_args()

    # ── 路径 ──
    crypto_path = CryptoPath(crypto_path=Path(args.db_path) / "crypto")
    register_marketcap_paths(crypto_path)

    date_tag = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_dir = crypto_path.crypto / "marketcap" / "daily"
    binance_dir = crypto_path.crypto / "marketcap" / "binance"
    full_file = daily_dir / f"{date_tag}.parquet"
    binance_file = binance_dir / f"{date_tag}.parquet"

    if full_file.exists() and not args.force:
        logger.info(f"{full_file} exists, skip (--force to re-fetch)")
        return

    # ── 1. 拉取 CoinGecko 全量市场数据（按市值排序）──
    logger.info("Fetching all CoinGecko market data...")
    df_markets = fetch_all_markets()
    logger.info(f"Market data shape: {df_markets.shape}")

    # ── 2. 从已排序的市场数据构建 ticker 映射 ──
    #     /coins/markets 已按 market_cap_desc 排列，每个 ticker 第一次出现即
    #     市值最高的 coin，天然消歧。
    logger.info("Building ticker→coingecko_id map from market data...")
    ticker_map = build_ticker_map_from_markets(df_markets)
    logger.info(f"Ticker map: {len(ticker_map)} unique tickers")

    # ── 3. 获取 binance 标的列表并映射 ──
    logger.info("Resolving binance symbols to CoinGecko IDs...")
    crypto_db = CryptoDB(Path(args.db_path))
    binance_symbols = crypto_db.get_local_future_perpetual_symbol_list(
        AssetType.future_um
    )
    sym_to_cgid = resolve_binance_to_coingecko(binance_symbols, ticker_map)

    # 打印前 20 个映射用于验证
    for i, (sym, cgid) in enumerate(sorted(sym_to_cgid.items())):
        if i < 20:
            logger.info(f"  {sym:15s} → {cgid}")
    logger.info(
        f"Resolved {len(sym_to_cgid)}/{len(binance_symbols)} binance symbols"
    )

    # ── 4. 按映射过滤出我们关心的标的 ──
    cg_ids_in_binance = set(sym_to_cgid.values())
    mapping_records = [
        {"coingecko_id": cg_id, "binance_symbol": sym}
        for sym, cg_id in sym_to_cgid.items()
    ]
    df_mapping = pl.DataFrame(mapping_records)

    df_binance = df_markets.filter(
        pl.col("coingecko_id").is_in(cg_ids_in_binance)
    ).join(df_mapping, on="coingecko_id", how="left")

    # ── 5. 写入 parquet ──
    df_markets.write_parquet(full_file)
    logger.success(f"Full snapshot → {full_file} ({len(df_markets)} coins)")

    if len(df_binance) > 0:
        df_binance.write_parquet(binance_file)
        logger.success(
            f"Binance filtered → {binance_file} ({len(df_binance)} rows)"
        )
    else:
        logger.warning("No binance symbols matched, skip writing binance file")

    logger.success(
        f"Tracked {len(binance_symbols)} binance symbols, "
        f"matched {len(sym_to_cgid)} to CoinGecko"
    )


if __name__ == "__main__":
    main()
