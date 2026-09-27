#!/usr/bin/env python3
"""Resume-safe Binance USDT archive repair, compatible with existing CryptoDB parquet.

Run with the project's .venv Python. End is exclusive (UTC); defaults to today.
Monthly archives take precedence; daily archives cover every missing month.
No synthetic candles, zero-volume filtering, or forward filling is performed.
"""
from __future__ import annotations

import argparse
import calendar
import concurrent.futures as cf
import datetime as dt
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import shutil
import threading
import time
import xml.etree.ElementTree as ET
import zipfile

os.environ.setdefault("POLARS_MAX_THREADS", "4")
import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
CDN = "https://data.binance.vision/"
NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
KCOLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
         "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]
MCOLS = ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
         "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
         "count_long_short_ratio", "sum_taker_long_short_vol_ratio"]
FCOLS = ["calc_time", "funding_interval_hours", "last_funding_rate"]
LOCAL = threading.local()


def session(proxy):
    if not hasattr(LOCAL, "session"):
        s = requests.Session()
        s.trust_env = False
        if proxy:
            s.proxies = {"http": proxy, "https": proxy}
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        LOCAL.session = s
    return LOCAL.session


def get(url, proxy, **kwargs):
    r = session(proxy).get(url, timeout=(15, 90), **kwargs)
    r.raise_for_status()
    return r


def listing(prefix, proxy, directories=False):
    params = {"prefix": prefix, "delimiter": "/", "max-keys": 1000}
    result = []
    while True:
        root = ET.fromstring(get(S3, proxy, params=params).content)
        keys = [e.text for e in root.findall("s:Contents/s:Key", NS)]
        dirs = [e.text for e in root.findall("s:CommonPrefixes/s:Prefix", NS)]
        result.extend(dirs if directories else [k for k in keys if k.endswith(".zip")])
        if root.findtext("s:IsTruncated", namespaces=NS) != "true":
            break
        marker = root.findtext("s:NextMarker", namespaces=NS) or max(keys + dirs)
        if marker == params.get("marker"):
            raise RuntimeError("S3 pagination stalled")
        params["marker"] = marker
    return result


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def stamp(key, symbol, kind, interval):
    head = f"{symbol}-{interval if kind == 'klines' else kind}-"
    return Path(key).stem.removeprefix(head)


def choose_archives(monthly, daily, symbol, kind, interval, start, end):
    selected = []
    months = set()
    for k in monthly:
        label = stamp(k, symbol, kind, interval)
        day = dt.date.fromisoformat(label + "-01")
        following = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        if day < end and following > start:
            selected.append(k)
            months.add(label)
    for k in daily:
        label = stamp(k, symbol, kind, interval)
        day = dt.date.fromisoformat(label)
        if start <= day < end and label[:7] not in months:
            selected.append(k)
    return sorted(selected)


def parse_archive(content, kind, symbol):
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if n.endswith(".csv")]
        if len(names) != 1:
            raise ValueError("Expected exactly one CSV")
        data = z.read(names[0])  # ZIP CRC is verified while reading.
    cols = KCOLS if kind == "klines" else MCOLS if kind == "metrics" else FCOLS
    first = data.split(b"\n", 1)[0].decode("utf-8-sig").split(",")[0]
    header = first == cols[0]
    schema = {c: pl.Float64 for c in cols}
    if kind == "klines":
        schema.update({c: pl.Int64 for c in ["open_time", "close_time", "count"]})
    elif kind == "metrics":
        schema.update(create_time=pl.String, symbol=pl.String)
    else:
        schema.update(calc_time=pl.Int64, funding_interval_hours=pl.Int8)
    frame = pl.read_csv(data, has_header=header, new_columns=cols, schema_overrides=schema)
    if frame.width != len(cols):
        raise ValueError("Unexpected CSV columns")
    if kind == "klines":
        frame = frame.drop("ignore").with_columns([
            pl.when(pl.col(c) >= 100_000_000_000_000)
            .then(pl.col(c)).otherwise(pl.col(c) * 1000).cast(pl.Datetime("us")).alias(c)
            for c in ["open_time", "close_time"]
        ])
    elif kind == "metrics":
        frame = frame.with_columns(pl.col("create_time").str.to_datetime("%Y-%m-%d %H:%M:%S"))
        if frame.filter(pl.col("symbol") != symbol).height:
            raise ValueError("Metrics symbol mismatch")
        frame = frame.drop("symbol")
    else:
        frame = frame.with_columns(pl.from_epoch("calc_time", time_unit="ms"))
    return frame.with_columns(jj_code=pl.lit(symbol))


def load_archive(key, args, kind, symbol):
    path = args.db_path / "crypto" / key
    if path.exists():
        try:
            return parse_archive(path.read_bytes(), kind, symbol)
        except Exception:
            logging.warning("Invalid cached ZIP, downloading again: %s", path)
    if shutil.disk_usage(args.db_path).free < args.min_free_gb * 1024**3:
        raise RuntimeError("Disk free space below configured minimum")
    content = get(CDN + key, args.proxy).content
    expected = get(CDN + key + ".CHECKSUM", args.proxy).text.split()[0]
    if hashlib.sha256(content).hexdigest().lower() != expected.lower():
        raise ValueError(f"SHA256 mismatch: {key}")
    frame = parse_archive(content, kind, symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".zip.part")
    tmp.write_bytes(content)
    os.replace(tmp, path)
    return frame


def refresh_info(args):
    folder = args.db_path / "crypto/info"
    folder.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, url in [("Spot", "https://api.binance.com/api/v3/exchangeInfo"),
                      ("UMFutures", "https://fapi.binance.com/fapi/v1/exchangeInfo")]:
        data = get(url, args.proxy).json()
        if not data.get("symbols"):
            raise ValueError(f"Empty exchange information: {name}")
        atomic_json(folder / f"{name}.exchangeInfo.json", data)
        rows = []
        for entry in data["symbols"]:
            row = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                   for k, v in entry.items()}
            row["update_datetime"] = dt.datetime.now(dt.timezone.utc).isoformat()
            for c in ["onboardDate", "deliveryDate"]:
                if c in row:
                    row[c] = dt.datetime.fromtimestamp(row[c] / 1000, dt.timezone.utc).replace(tzinfo=None).isoformat()
            if "contractType" in row:
                row["contract_type"] = row["contractType"]
            rows.append(row)
        target = folder / f"{name}.csv"
        if target.exists():
            backup = folder / "backfill_backups" / f"{name}.{args.run_id}.csv"
            backup.parent.mkdir(exist_ok=True)
            shutil.copy2(target, backup)
        tmp = target.with_suffix(".csv.tmp")
        pl.DataFrame(rows, infer_schema_length=None).write_csv(tmp)
        os.replace(tmp, target)
        if name == "UMFutures":
            shutil.copy2(target, folder / "DerivativesTradingUsdsFutures.csv")
        counts[name] = len(rows)
        logging.info("Exchange information refreshed: %s (%s symbols)", name, len(rows))
    return counts


def output_path(args, market, kind, interval, symbol):
    relative = f"kline/{market}/{interval}" if kind == "klines" else (
        "metrics/um/5m" if kind == "metrics" else "funding_rate/um/8h")
    return args.db_path / "crypto" / relative / f"{symbol}.parquet"


def discover(args, market, kind):
    asset = "spot" if market == "spot" else "futures/um"
    freqs = ["daily"] if kind == "metrics" else ["monthly", "daily"] if kind == "klines" else ["monthly"]
    symbols = set()
    for freq in freqs:
        symbols.update(p.rstrip("/").split("/")[-1]
                       for p in listing(f"data/{asset}/{freq}/{kind}/", args.proxy, True))
    return sorted(s for s in symbols if (args.all_quotes or s.endswith("USDT"))
                  and (not args.symbols or s in args.symbols))


def scoped_symbols(args, market, kind, discovered):
    """Spot is auxiliary data for the exact UM symbol universe, including history."""
    key = (market, kind)
    if key not in discovered:
        symbols = discover(args, market, kind)
        if market == "spot":
            um_symbols = set(scoped_symbols(args, "um", "klines", discovered))
            symbols = [symbol for symbol in symbols if symbol in um_symbols]
        discovered[key] = symbols
    return discovered[key]


def full_periods(existing, column, interval):
    if existing is None:
        return {}, {}
    counts = existing.select(column).unique().group_by(pl.col(column).dt.date().alias("date")).len()
    daily = dict(counts.iter_rows())
    monthly = {}
    for day, count in daily.items():
        label = day.strftime("%Y-%m")
        monthly[label] = monthly.get(label, 0) + count
    return daily, monthly


def kline_tail(args, market, symbol, interval, latest):
    """Bridge recent archive publication lag using completed REST candles only."""
    end = dt.datetime.combine(args.end, dt.time())
    step = {"1m": 60, "1h": 3600, "1d": 86400}[interval]
    begin = latest + dt.timedelta(seconds=step)
    if begin >= end or begin < end - dt.timedelta(days=7):
        return None
    endpoint = ("https://api.binance.com/api/v3/klines" if market == "spot"
                else "https://fapi.binance.com/fapi/v1/klines")
    cursor = int(begin.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    end_ms = int(end.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    rows = []
    while cursor < end_ms:
        batch = get(endpoint, args.proxy, params={"symbol": symbol, "interval": interval,
                    "startTime": cursor, "endTime": end_ms - 1, "limit": 1000}).json()
        if not batch:
            break
        rows.extend(batch)
        new_cursor = int(batch[-1][0]) + step * 1000
        if new_cursor <= cursor:
            raise RuntimeError("Kline API pagination stalled")
        cursor = new_cursor
        time.sleep(0.15)
    if not rows:
        return None
    frame = pl.DataFrame(rows, schema=KCOLS, orient="row", infer_schema_length=None)
    frame = frame.with_columns([pl.col(c).cast(pl.Int64 if c in ["open_time", "close_time", "count"] else pl.Float64) for c in KCOLS])
    return frame.drop("ignore").with_columns(pl.from_epoch("open_time", time_unit="ms"),
               pl.from_epoch("close_time", time_unit="ms"), jj_code=pl.lit(symbol)).filter(
                   (pl.col("open_time") >= begin) & (pl.col("close_time") < end))


def repair_symbol(args, market, kind, interval, symbol):
    target = output_path(args, market, kind, interval, symbol)
    column = "open_time" if kind == "klines" else "create_time" if kind == "metrics" else "calc_time"
    ident = f"{market}_{kind}_{interval}_{symbol}"
    checkpoint = args.state / "checkpoints" / f"{ident}.json"
    existing = pl.read_parquet(target) if target.exists() else None
    previous = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    valid = (target.exists() and previous.get("file_size") == target.stat().st_size
             and previous.get("mtime_ns") == target.stat().st_mtime_ns
             and previous.get("start") == str(args.start) and previous.get("end") == str(args.end))
    done = set(previous.get("archives", [])) if valid else set()
    asset = "spot" if market == "spot" else "futures/um"
    suffix = f"/{interval}" if kind == "klines" else ""
    def files(freq):
        return listing(f"data/{asset}/{freq}/{kind}/{symbol}{suffix}/", args.proxy)
    monthly = [] if kind == "metrics" else files("monthly")
    daily = [] if kind == "fundingRate" else files("daily")
    keys = choose_archives(monthly, daily, symbol, kind, interval, args.start, args.end)
    dc, mc = full_periods(existing, column, interval)
    per_day = {"1m": 1440, "1h": 24, "1d": 1}.get(interval, 288) if kind != "fundingRate" else None
    pending = []
    for key in keys:
        label = stamp(key, symbol, kind, interval)
        complete = False
        if per_day:
            if len(label) == 7:
                y, m = map(int, label.split("-"))
                complete = mc.get(label, 0) == calendar.monthrange(y, m)[1] * per_day
            else:
                complete = dc.get(dt.date.fromisoformat(label), 0) == per_day
        if key not in done and not complete:
            pending.append(key)
    logging.info("START %s archives=%d pending=%d existing_rows=%d", ident, len(keys), len(pending), 0 if existing is None else existing.height)
    chunks = []
    # Bound the number of in-flight archive downloads independently of total history.
    with cf.ThreadPoolExecutor(max_workers=args.download_workers) as pool:
        for offset in range(0, len(pending), 24):
            batch = pending[offset:offset + 24]
            for frame in pool.map(lambda k: load_archive(k, args, kind, symbol), batch):
                frame = frame.filter((pl.col(column) >= dt.datetime.combine(args.start, dt.time())) &
                                     (pl.col(column) < dt.datetime.combine(args.end, dt.time())))
                chunks.append(frame)
            if offset and offset % 240 == 0:
                logging.info("PROGRESS %s archives=%d/%d", ident, offset + len(batch), len(pending))
    if kind == "klines" and symbol in args.active_symbols.get(market, set()):
        times = [f[column].max() for f in chunks if f.height]
        if existing is not None and existing.height:
            times.append(existing[column].max())
        if times:
            tail = kline_tail(args, market, symbol, interval, max(times))
            if tail is not None:
                chunks.append(tail)
    if kind == "fundingRate" and args.funding_api:
        # Start at the archive boundary, not existing max: a partial previous run
        # may already contain newer events while still having an interior gap.
        archive_months = [stamp(k, symbol, kind, interval) for k in keys]
        latest = dt.datetime.combine(args.start, dt.time())
        if archive_months:
            month = dt.date.fromisoformat(max(archive_months) + "-01")
            following = (month.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
            latest = max(latest, dt.datetime.combine(following, dt.time()))
        cursor = int(latest.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        end_ms = int(dt.datetime.combine(args.end, dt.time(), dt.timezone.utc).timestamp() * 1000)
        records = []
        while cursor < end_ms:
            time.sleep(0.6)
            rows = get("https://fapi.binance.com/fapi/v1/fundingRate", args.proxy,
                       params={"symbol": symbol, "startTime": cursor, "endTime": end_ms - 1, "limit": 1000}).json()
            if not rows:
                break
            for r in rows:
                records.append({"calc_time": dt.datetime.fromtimestamp(r["fundingTime"] / 1000, dt.timezone.utc).replace(tzinfo=None),
                                "funding_interval_hours": None, "last_funding_rate": float(r["fundingRate"]), "jj_code": symbol})
            new_cursor = int(rows[-1]["fundingTime"]) + 1
            if new_cursor <= cursor:
                raise RuntimeError("Funding API pagination stalled")
            cursor = new_cursor
        if records:
            api = pl.DataFrame(records).with_columns(pl.col("funding_interval_hours").cast(pl.Int8))
            # Do not replace a known archive interval with an API null.
            known = [f.select(column).cast({column: pl.Datetime("us")}) for f in
                     (([existing] if existing is not None else []) + chunks) if f.height]
            if known:
                api = api.join(pl.concat(known).unique(), on=column, how="anti")
            chunks.append(api)
    frames = ([existing] if existing is not None else []) + chunks
    if not frames:
        return {"id": ident, "status": "no_archive", "archives": len(keys)}
    frame = pl.concat(frames, how="diagonal_relaxed").unique(subset=[column], keep="last").sort(column)
    if frame[column].null_count():
        raise ValueError("Null timestamps")
    if existing is not None and frame.height < existing[column].n_unique():
        raise ValueError("Merge would lose existing timestamps")
    target.parent.mkdir(parents=True, exist_ok=True)
    if chunks:
        if shutil.disk_usage(args.db_path).free < args.min_free_gb * 1024**3:
            raise RuntimeError("Disk free space below configured minimum")
        tmp = target.with_suffix(".parquet.tmp")
        frame.write_parquet(tmp, compression="zstd")
        check = pl.scan_parquet(tmp).select(pl.len(), pl.col(column).min().alias("min"), pl.col(column).max().alias("max")).collect()
        if check["len"][0] != frame.height:
            raise ValueError("Written parquet row count mismatch")
        os.replace(tmp, target)
    missing = None
    if kind == "klines" and frame.height:
        step = {"1m": 60, "1h": 3600, "1d": 86400}[interval]
        missing = frame.select(((pl.col(column).diff().dt.total_seconds() // step - 1).clip(lower_bound=0)).sum()).item()
    result = {"id": ident, "status": "ok", "rows": frame.height,
              "min": frame[column].min(), "max": frame[column].max(), "archives": len(keys),
              "downloaded_or_loaded": len(pending), "remaining_gap_slots": missing,
              "file_size": target.stat().st_size, "mtime_ns": target.stat().st_mtime_ns}
    atomic_json(checkpoint, {**result, "start": args.start, "end": args.end,
                             "archives": sorted(done | set(pending))})
    logging.info("DONE %s rows=%d max=%s gap_slots=%s", ident, frame.height, result["max"], missing)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=Path("E:/DuckDB"))
    p.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2020, 1, 1))
    p.add_argument("--end", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date())
    p.add_argument("--proxy", default="http://127.0.0.1:7890")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--minute-workers", type=int, default=2)
    p.add_argument("--download-workers", type=int, default=4)
    p.add_argument("--min-free-gb", type=int, default=30)
    p.add_argument("--symbols", nargs="*")
    p.add_argument("--all-quotes", action="store_true")
    p.add_argument("--phases", nargs="+", default=["1d", "1h", "funding", "metrics", "1m"], choices=["1d", "1h", "1m", "metrics", "funding"])
    p.add_argument("--skip-info", action="store_true")
    p.add_argument("--funding-api", action=argparse.BooleanOptionalAction, default=True)
    args = p.parse_args()
    if args.start >= args.end or min(args.workers, args.minute_workers, args.download_workers) < 1:
        p.error("Invalid dates or worker count")
    args.run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    args.state = args.db_path / "crypto/backfill_state"
    args.state.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(args.state / f"run_{args.run_id}.log", encoding="utf-8")])
    # OS releases this lock even after an unclean exit; stale files are harmless.
    lock_file = (args.state / "run.lock").open("a+b")
    lock_file.seek(0)
    lock_file.write(b"0")
    lock_file.flush()
    lock_file.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    results = []
    status = {"run_id": args.run_id, "pid": os.getpid(), "status": "running", "start": args.start, "end_exclusive": args.end, "completed": 0, "failed": 0}
    atomic_json(args.state / "status.json", status)
    try:
        if not args.skip_info:
            status["exchange_info"] = refresh_info(args)
        args.active_symbols = {}
        for market, name in [("spot", "Spot"), ("um", "UMFutures")]:
            info = args.db_path / "crypto/info" / f"{name}.exchangeInfo.json"
            args.active_symbols[market] = {s["symbol"] for s in json.loads(info.read_text(encoding="utf-8"))["symbols"]
                                           if s.get("status") == "TRADING"} if info.exists() else set()
        discovered = {}
        for phase in args.phases:
            kind = "metrics" if phase == "metrics" else "fundingRate" if phase == "funding" else "klines"
            interval = phase if kind == "klines" else "5m" if kind == "metrics" else "8h"
            jobs = []
            for market in (["um"] if kind != "klines" else ["um", "spot"]):
                symbols = scoped_symbols(args, market, kind, discovered)
                # Major assets first to make progress easy to verify.
                symbols = sorted(symbols, key=lambda s: (s not in ["BTCUSDT", "ETHUSDT"], s))
                jobs.extend((market, kind, interval, symbol) for symbol in symbols)
            status.update(phase=phase, phase_total=len(jobs), phase_completed=0,
                          spot_scope="exact UM archive symbols (including delisted)")
            atomic_json(args.state / f"universe_{phase}.json", {
                "run_id": args.run_id,
                "markets": {market: [j[3] for j in jobs if j[0] == market]
                            for market in sorted({j[0] for j in jobs})}})
            atomic_json(args.state / "status.json", status)
            logging.info("PHASE %s jobs=%d", phase, len(jobs))
            workers = args.minute_workers if phase == "1m" else (1 if phase == "funding" else args.workers)
            with cf.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(repair_symbol, args, *job): job for job in jobs}
                for f in cf.as_completed(futures):
                    job = futures[f]
                    try:
                        result = f.result()
                        status["completed"] += 1
                    except Exception as e:
                        logging.exception("FAILED %s", job)
                        result = {"id": "_".join(job), "status": "failed", "error": str(e)}
                        status["failed"] += 1
                    results.append(result)
                    with (args.state / f"results_{args.run_id}.jsonl").open("a", encoding="utf-8") as out:
                        out.write(json.dumps(result, default=str) + "\n")
                    status["phase_completed"] += 1
                    status["last_result"] = result
                    atomic_json(args.state / "status.json", status)
        status["status"] = "completed" if status["failed"] == 0 else "completed_with_errors"
    except BaseException as e:
        status.update(status="failed", error=str(e))
        raise
    finally:
        atomic_json(args.state / "status.json", status)
        lock_file.close()
    return int(status["failed"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
