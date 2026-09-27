# USDT historical data backfill

Run from PowerShell:

```powershell
& E:\gubo\pond\.venv\Scripts\python.exe E:\gubo\pond\pond\duckdb\crypto\scripts\backfill_all.py --db-path E:\DuckDB --workers 6
```

Defaults: USDT symbols including delisted symbols found in Binance archives;
2020-01-01 through the previous complete UTC day; phases 1d, 1h, funding,
metrics, 1m. Existing rows before the requested start are retained. Spot is
restricted to the intersection of its archive symbols and the UM kline symbol
universe, including historical/delisted UM symbols. Exact symbol names are
matched; no 1000-prefixed contract mapping is guessed. Spot-only symbols are
not downloaded. `backfill_state/universe_{phase}.json` records each phase's
selected symbols. Spot does not have the UM
open-interest metrics or perpetual funding dataset.

Outputs retain the existing layout:

- `crypto/kline/{spot,um}/{1d,1h,1m}/{symbol}.parquet`
- `crypto/metrics/um/5m/{symbol}.parquet`
- `crypto/funding_rate/um/8h/{symbol}.parquet`
- `crypto/info/{Spot,UMFutures}.csv` and corresponding `.exchangeInfo.json`
- `crypto/info/DerivativesTradingUsdsFutures.csv` for the current SDK reader

The `8h` funding directory is a compatibility name, not a fixed cadence.
Archive intervals are preserved. Funding events obtained from the REST API
have a null `funding_interval_hours`, because that API does not provide the
historical interval. No interval is guessed.

The downloader reuses cached ZIPs, verifies newly downloaded SHA256 checksums,
preserves zero-volume bars and spot microsecond timestamps, deduplicates by
timestamp, and atomically replaces parquet after verifying its row count.
Daily archives fill months without monthly archives. Recent completed candles
for currently trading symbols are supplemented by REST when the last local
or archive candle is within seven days of the requested end. Funding REST
starts at the final monthly archive boundary, including interior tail gaps.

Source gaps are not interpolated. `remaining_gap_slots` counts missing kline
slots between the first and last saved timestamps, including exchange outages
and trading suspensions; it does not assert that Binance has those candles.
Metrics availability is limited to published archives.

Progress and diagnostics:

```powershell
Get-Content E:\DuckDB\crypto\backfill_state\status.json
Get-Content E:\DuckDB\crypto\backfill_state\background.stderr.log -Tail 20
```

Each run also writes `run_YYYYMMDD_HHMMSS.log` and
`results_YYYYMMDD_HHMMSS.jsonl`. Status remains `running` during processing;
`completed_with_errors` means some symbol jobs failed and must be retried.
An OS lock prevents overlapping instances of this script. Stop gracefully
with Ctrl+C when running in a terminal. Rerun the same command to resume;
checkpoints are accepted only when the date range and output file match.
The default free-space floor is 30 GiB. New archive files and parquet temporary
files are committed only after successful validation; existing parquet is
preserved if a job fails before commit.

Useful options: `--symbols BTCUSDT ETHUSDT`, `--phases funding metrics`,
`--minute-workers 2`, `--download-workers 4`, `--skip-info`,
`--proxy http://127.0.0.1:7890`, `--proxy ''` to connect directly.

Offline checks:

```powershell
& E:\gubo\pond\.venv\Scripts\python.exe -m unittest discover -s E:\gubo\pond\tests -p test_backfill_all.py
```
