# pond.duckdb.crypto — CryptoDB（币安加密行情数据）

基于 DuckDB/Parquet 的币安（Binance）行情数据存储与下载工具：K线、trades、aggTrades、metrics、fundingRate、orderbook、marketcap 等数据统一以 parquet 落盘，按 `asset_type / data_type / timeframe` 组织目录。

## 配置（改路径只需要改这里）

所有 `CryptoDB` 实例的数据库根目录统一从本目录下的 **`.env`** 文件读取：

```ini
# pond/duckdb/crypto/.env
DB_PATH=/home/fangyang/HDD_GT580/Duckdb/
```

- 读取逻辑在 `path.py::get_db_path()`：优先 `.env` 里的 `DB_PATH`，`.env` 没有时回退到环境变量 `DB_PATH`，都没有则报错。
- 不依赖 `python-dotenv`，纯文本解析。
- **换机器 / 换盘只需改 `.env` 这一处**，不要再在代码里硬编码路径。
- 脚本的 `--db-path` 命令行参数仍可临时覆盖（优先级高于 .env），用于一次性调试。
- **例外：`pond/clickhouse` 模块不走这个 .env**，它用环境变量 `CRYPTODB_PATH` 单独配置本地 DuckDB 路径（`pond/clickhouse/__init__.py::get_crypto_db_path()`），默认 `E:\DuckDB`。

数据实际存放位置：`{DB_PATH}/crypto/...`（`CryptoDB.__init__` 会自动 `mkdir` 出全部子目录）。

## 基本用法

```python
from pond.duckdb.crypto import CryptoDB, AssetType, DataType

# db_path 不传 → 自动读 .env 的 DB_PATH
db = CryptoDB(requests_proxies={          # 可选，需要翻墙时传
    "host": "127.0.0.1",
    "port": 7890,
    "protocol": "http",
})

# 下载/增量更新历史数据（多线程）
db.update_history_data_parallel(
    start="2020-1-1",
    end="2026-01-01",
    asset_type=AssetType.future_um,        # spot / future_um / future_cm
    data_type=DataType.klines,             # klines / trades / aggTrades / metrics / fundingRate
    timeframe="1h",
    httpx_proxies={"https://": "http://127.0.0.1:7890"},
    if_only_usdt=True,
    workers=8,
)

# 读取（本地没有的区间会自动从币安下载）
df = db.load_history_data(
    symbol="BTCUSDT",
    start=datetime(2020, 1, 1),
    end=datetime(2026, 1, 1),
    asset_type=AssetType.future_um,
    data_type=DataType.klines,
    timeframe="1h",
)
```

其他常用方法（见 `__init__.py`）：

| 方法 | 说明 |
| --- | --- |
| `db.get_future_info(asset_type, from_local=True)` | 合约/现货标的信息表（本地 CSV，48h 内不重拉） |
| `db.update_future_info(force=False)` | 从币安接口刷新标的信息 CSV |
| `db.get_local_future_perpetual_symbol_list(asset_type)` | 本地永续合约 symbol 列表 |
| `db.update_history_data(...)` | 单标的下载（非并发版） |
| `db.update_crypto_trades()` / `db.update_crypto_agg_trades()` | 更新 trades / aggTrades |

代理格式说明：`requests_proxies` 用 `{"host", "port", "protocol"}` 格式（内部会自动转成 requests/httpx 格式）；`update_history_data_parallel` 的 `httpx_proxies` 直接传 `{"https://": "http://127.0.0.1:7890"}`。

## 数据目录布局

由 `path.py::CryptoPath` 定义，`{DB_PATH}/crypto/` 下：

```
crypto/
├── info/                      # 标的信息 CSV（Spot.csv / DerivativesTradingUsdsFutures.csv）
├── data/
├── kline/{spot,um,cm}/{1m,5m,15m,1h,...}
├── trades/{origin,spot,um,cm}/...
├── agg_trades/{origin,spot,um,cm}/...
├── metrics/{um,cm}/5m/
├── funding_rate/{um,cm}/8h/
├── orderbook/
└── marketcap/{daily,binance}/{YYYY-MM-DD}.parquet   # 由 fetch_marketcap.py 生成
```

`asset_type`（`pond.binance_history.type.AssetType`）：`spot` / `future_um`（USDT 本位合约，USDT-margined）/ `future_cm`（币本位合约，coin-margined）。
`data_type`（`DataType`）：`klines` / `trades` / `aggTrades` / `metrics` / `fundingRate`。

## scripts/ 脚本

| 脚本 | 作用 |
| --- | --- |
| `download_kline.py` | 全量增量下载 kline（直接运行脚本，无参数，改脚本顶部配置） |
| `download_metric.py` | 全量增量下载 metrics |
| `download_future_1m_selected.py` | 下载指定永续合约的 1m kline，`--symbols --start --end --db-path --workers --ignore-cache --disable-proxy` |
| `fetch_marketcap.py` | 拉 CoinGecko 全市场市值快照存 parquet，`--force --date --db-path` |
| `etl_to_postgresql.py` | 把 kline 数据 ETL 到 PostgreSQL（断点续传），需同目录 `config.toml`（host/port/dbname/user/password） |
| `export_marketcap_csv.py` | 从 PostgreSQL marketcap 表导出 CSV，复用 `config.toml` |

注意：

- `etl_to_postgresql.py` / `export_marketcap_csv.py` 依赖 `scripts/config.toml`（数据库连接），该文件含密码，不要提交到 git。
- 合约标的以 `UMFutures_manual.csv`（本目录）+ 币安接口为准，`update_history_data_parallel` 会自动合并。
- `liang/` 下是手动放置的临时数据，不属于 CryptoDB 管理范围。

## 给后续 agent 的提示

- 需要数据库路径时用 `from pond.duckdb.crypto.path import get_db_path`，不要自己写死路径。
- 新建脚本默认 `CryptoDB()` 不传 `db_path`；如需临时覆盖再传 `Path(...)`。
- 修改 `.env` 的 `DB_PATH` 后，`init_db_path` 会自动建好目录树。
