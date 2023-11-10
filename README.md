pond
========================
<div align="center">
<img src="https://github.com/FangyangJz/pond/assets/19723117/f0b39772-284f-407b-8452-31d0e9583a5a?raw=true">
</div>

## Description

**`pond`** is an open-source integrated finance data source and database application on top of [DuckDB](https://duckdb.org/) and [akshare](https://github.com/akfamily/akshare).

### Installation

If you plan to develop gulf yourself, or want to be on the cutting edge, you can use an editable install:
```bash
git clone https://github.com/FangyangJz/pond.git
pip install -e .
```

## Quickstart

>Note: Initial DB path need mkdir first manually.

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

# need csv file in D:\DuckDB\stock\trades\origin
# transform csv to parquet file reduct for reducting file size
db.update_stock_trades()

# read stock basic info from disk
df1 = db.stock_basic_df

# read trade calender dataframe
df2 = db.calender_df

# read restoration stock daily kilne data from disk
df3 = db.get_kline_1d_qfq_df()
```

### 3. CryptoDB
```python
from pathlib import Path
from pond.duckdb.crypto import CryptoDB

# assign a directory of path and init BondDB 
# db = CryptoDB(Path(r'/home/fangyang/zhitai5000/DuckDB/'))
db = CryptoDB(Path(r'D:\DuckDB'))

# extract csv file in D:\DuckDB\crypto\trades\origin
# update crypto trade data from csv file to parquet
db.update_crypto_trades()

# extract csv file in D:\DuckDB\crypto\agg_trades\origin
# update crypto aggTrade data from csv file to parquet
db.update_crypto_agg_trades()
```