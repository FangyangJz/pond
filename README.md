pond
========================
<div align="center">
<img src="https://github.com/FangyangJz/pond/assets/19723117/f0b39772-284f-407b-8452-31d0e9583a5a?raw=true">
</div>

## Description

**`pond`** is an open-source integrated finance data source and database application on top of [DuckDB](https://duckdb.org/) and [akshare](https://github.com/akfamily/akshare).

### Installation

If you plan to develop pond yourself, or want to be on the cutting edge, you can use an editable install:
```bash
$ git clone https://github.com/FangyangJz/pond.git
$ cd pond
$ uv sync
```
>Note: `Pycharm` and `vscode` use `Black` as code formatter.

## Quickstart

>Note: Initial DB path need mkdir manually first.

### 1. BondDB
```python
from pathlib import Path
from pond.duckdb.bond import BondDB

# assign a directory of path and init BondDB
db = BondDB(Path(r'D:\DuckDB'))

# download bond info and redeem data
db.update_bond_info()

# save bond daily kline by reading local data
db.update_bond_kline_1d()

# read bond daily kline data from disk
df = db.kline_1d_df()
```

### 2. StockDB
```python
from pathlib import Path
from pond.duckdb.stock import StockDB

# assign a directory of path and init StockDB
db = StockDB(Path(r'D:\DuckDB'))

# download stock info data and calender data
db.update_stock_info()

# save no restoration stock daily kilne by reading local data
# may be useless because restoration stock daily kilne include origin data
db.update_kline_1d_nfq()

# save restoration stock daily kilne by reading local data
db.update_kline_1d_qfq()

# need wind level2 csv file in D:\DuckDB\stock\level2\origin
# transform csv to parquet file for reducting file size
db.update_level2_trade()
db.update_level2_order()
db.update_level2_orderbook()

# read stock basic info from disk
df1 = db.stock_basic_df

# read trade calender dataframe
df2 = db.calender_df

# read restoration stock daily kilne data from disk
df3 = db.get_kline_1d_qfq_df()
```

### 3. CryptoDB
Usage examples:
```python
import polars as pl
from pathlib import Path
from pond.duckdb.crypto import CryptoDB

# assign a directory of path and init BondDB
# db = CryptoDB(Path(r'/home/fangyang/zhitai5000/DuckDB/'))
db = CryptoDB(Path(r'D:\DuckDB'))

# update Spot, CM and UM future info in E:\DuckDB\crypto\info
db.update_future_info()

# download history future data to D:\DuckDB\crypto\data
# save parquet file in E:\DuckDB\crypto\kline
# In my case, close proxy software (clash) to avoid TLS handshake error.
db.update_history_data(*params)

# read parquet file in E:\DuckDB\crypto\kline directory
df = pl.read_parquet(db.path_crypto_kline_um/ '1m' /'BTCUSDT.parquet')

# extract csv file in D:\DuckDB\crypto\trades\origin
# update crypto trade data from csv file to parquet
db.update_crypto_trades()

# extract csv file in D:\DuckDB\crypto\agg_trades\origin
# update crypto aggTrade data from csv file to parquet
db.update_crypto_agg_trades()
```

## Ops Banchmark

If you are still hesitant about which computing framework to choose. Please refer this link [Database-like ops benchmark](https://duckdblabs.github.io/db-benchmark/)

## Note
Dependencies must be `binance-connector==3.11.0`, if not binance-spot will raise import error.
