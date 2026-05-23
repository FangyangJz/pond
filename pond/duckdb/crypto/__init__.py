# !/usr/bin/env python3
# @Datetime : 2023/11/9 0:09
# @Author   : Fangyang
# @Software : PyCharm
import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import polars as pl
import pandas as pd
from tqdm import tqdm
from loguru import logger
from dateutil import parser, tz

from binance_sdk_spot import Spot
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures
from binance_common.configuration import ConfigurationRestAPI

from pond.duckdb import DuckDB, DataFrameStrType, df_types
from pond.binance_history.type import TIMEFRAMES, AssetType, DataType
from pond.duckdb.crypto.const import timeframe_data_types_dict
from pond.duckdb.crypto.path import CryptoPath
from pond.binance_history.utils import get_urls_by_xml_parse, load_data_from_disk
from pond.binance_history.async_api import start_async_download_files


class CryptoDB(DuckDB):
    def __init__(
        self,
        db_path: Path,
        requests_proxies: dict | None = None,
        df_type: DataFrameStrType = df_types.polars,
    ):
        self.crypto_path = CryptoPath(crypto_path=db_path / "crypto")
        self.init_db_path = self.crypto_path.init_db_path
        self.requests_proxies = requests_proxies or {}
        super().__init__(db_path, df_type)

    @staticmethod
    def _convert_proxies_for_requests(proxies: dict | None) -> dict[str, str]:
        """Convert new SDK proxy format to requests library format.

        New SDK format: {"host": "127.0.0.1", "port": 7890, "protocol": "http"}
        Requests format: {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
        """
        if not proxies or "host" not in proxies:
            return {}

        host = proxies.get("host", "")
        port = proxies.get("port", "")
        protocol = proxies.get("protocol", "http")
        proxy_url = f"{protocol}://{host}:{port}"

        return {"http": proxy_url, "https": proxy_url}

    def get_local_future_perpetual_symbol_list(
        self, asset_type: AssetType
    ) -> list[str]:
        df = self.get_future_info(asset_type)
        df = df[df["contract_type"].isin(["PERPETUAL", "TRADIFI_PERPETUAL"])]
        return df.symbol.to_list()

    @staticmethod
    def is_future_type(asset_type: AssetType):
        return asset_type == AssetType.future_um

    def get_future_info(self, asset_type: AssetType, from_local=True) -> pd.DataFrame:
        client = self.get_client(asset_type)
        file = self.crypto_path.info / f"{client.__class__.__name__}.csv"

        if from_local:
            if file.exists():
                df = pd.read_csv(file)
                # 兼容旧 CSV 文件的列名
                if "contractType" in df.columns and "contract_type" not in df.columns:
                    df = df.rename(columns={"contractType": "contract_type"})
                return df

        self.update_future_info()

        df = pd.read_csv(file)
        # 兼容旧 CSV 文件的列名
        if "contractType" in df.columns and "contract_type" not in df.columns:
            df = df.rename(columns={"contractType": "contract_type"})
        return df

    def update_future_info(self, force: bool = False):
        """
        字段说明: https://binance-docs.github.io/apidocs/futures/cn/#0f3f2d5ee7

        params: force=True 强制更新，忽略缓存
        """
        from pond.duckdb.crypto.future import get_future_info_df

        config = ConfigurationRestAPI(proxy=self.requests_proxies) if self.requests_proxies else None

        for c in [
            DerivativesTradingUsdsFutures(config_rest_api=config),
            Spot(config_rest_api=config),
        ]:
            file_path = self.crypto_path.info / f"{c.__class__.__name__}.csv"

            # 检查文件是否存在且在48小时内已更新
            if not force and file_path.exists():
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if (datetime.now() - file_mtime).total_seconds() < 48 * 3600:
                    logger.info(f"{file_path.name} is up-to-date (modified: {file_mtime}), skip download.")
                    continue

            logger.info(f"Updating {file_path} from network...")
            info_df = get_future_info_df(c)
            info_df.to_csv(file_path, index_label=False)

    def get_client(
        self, asset_type: AssetType, proxies: dict | None = None
    ) -> DerivativesTradingUsdsFutures | Spot:
        config = ConfigurationRestAPI(proxy=proxies) if proxies else None
        match asset_type:
            case AssetType.future_um:
                return DerivativesTradingUsdsFutures(config_rest_api=config)
            case AssetType.spot:
                return Spot(config_rest_api=config)
            case _:
                raise ValueError(f"{asset_type} is a wrong {AssetType}")

    @staticmethod
    def get_csv_schema(data_type: DataType) -> dict[str, pl.DataType]:
        from pond.duckdb.crypto.const import (
            klines_schema,
            metrics_schema,
            fundingRate_schema,
        )

        return {
            DataType.klines: klines_schema,
            DataType.metrics: metrics_schema,
            DataType.trades: {
                "id": pl.Int64,
                "price": pl.Float64,
                "qty": pl.Float64,
                "quoteQty": pl.Float64,
                "time": pl.Int64,
                "isBuyerMaker": pl.Boolean,
                "isBestMatch": pl.Boolean,
            },
            # DataType.aggTrades: {
            #     "a": pl.Int64,
            #     "p": pl.Float64,
            #     "q": pl.Float64,
            #     "f": pl.Int64,
            #     "l": pl.Int64,
            #     "T": pl.Int64,
            #     "m": pl.Boolean,
            #     "M": pl.Boolean,
            # },
            DataType.aggTrades: {
                "agg_trade_id": pl.Int64,
                "price": pl.Float64,
                "quantity": pl.Float64,
                "first_trade_id": pl.Int64,
                "last_trade_id": pl.Int64,
                "transact_time": pl.Int64,
                "is_buyer_maker": pl.Boolean,
            },
            DataType.fundingRate: fundingRate_schema,
        }[data_type]

    @staticmethod
    def filter_quote_volume_0(df: pl.DataFrame, symbol: str, timeframe: TIMEFRAMES):
        origin_df_len = len(df)
        df = (
            df.filter(pl.col("quote_volume") > 0)
            if timeframe not in ["1s", "1m"]
            else df
        )
        if (filtered_rows := origin_df_len - len(df)) > 0:
            logger.warning(f"[{symbol}] filter(quote_volume==0): {filtered_rows} rows.")

        return df

    def update_history_data_parallel(
        self,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        httpx_proxies: dict[str, str] = {},
        skip_symbols: list[str] = [],
        do_filter_quote_volume_0: bool = False,
        if_skip_usdc: bool = True,
        ignore_cache=False,
        workers=None,
    ):
        df = self.get_future_info(asset_type, from_local=False)
        if self.is_future_type(asset_type):
            df = df[df["contract_type"].isin(["PERPETUAL", "TRADIFI_PERPETUAL"])][
                [
                    "symbol",
                    "contract_type",
                    "deliveryDate",
                    "onboardDate",
                    "update_datetime",
                ]
            ]
            manual_df = pd.read_csv(Path(__file__).parent / "UMFutures_manual.csv")
            df = pd.concat([df, manual_df])
        else:
            # TODO spot 目前也走这里, 主要还是以合约标的为主
            df = self.get_future_info(AssetType.future_um, from_local=False)
            df = df[df["contract_type"].isin(["PERPETUAL", "TRADIFI_PERPETUAL"])][
                [
                    "symbol",
                    "contract_type",
                    "deliveryDate",
                    "onboardDate",
                    "update_datetime",
                ]
            ]
            manual_df = pd.read_csv(Path(__file__).parent / "UMFutures_manual.csv")
            df = pd.concat([df, manual_df])

        df = df.sort_values(by="symbol").drop_duplicates(subset=["symbol"])
        # 自动均分DataFrame，无需计算task_size
        chunks = np.array_split(df, workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    self.update_history_data,
                    chunk,
                    start,
                    end,
                    asset_type,
                    data_type,
                    timeframe,
                    httpx_proxies,
                    skip_symbols,
                    do_filter_quote_volume_0,
                    if_skip_usdc,
                    ignore_cache,
                    i,
                )
                for i, chunk in enumerate(chunks)
            ]

            # 等待所有任务完成，失败的任务记录错误但不中断其他任务
            failed_workers = []
            for i, future in enumerate(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker_{i} failed: {e}")
                    failed_workers.append(i)

            if failed_workers:
                logger.warning(f"Completed with {len(failed_workers)} failed workers: {failed_workers}")
            else:
                logger.success(f"All {len(futures)} workers completed successfully")

    def update_history_data(
        self,
        asset_info_df: pd.DataFrame,
        start: str = "2023-1-1",
        end: str = "2023-11-1",
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        httpx_proxies: dict[str, str] = {},
        skip_symbols: list[str] = [],
        do_filter_quote_volume_0: bool = False,
        if_skip_usdc: bool = True,
        ignore_cache=False,
        worker_id: int = 0,
    ):
        assert isinstance(asset_type, AssetType)
        assert isinstance(data_type, DataType)
        assert data_type in timeframe_data_types_dict[timeframe]

        base_path = self.crypto_path.get_base_path(asset_type, data_type)
        # 根据数据类型确定存储路径
        if data_type == DataType.klines:
            storage_path = base_path / timeframe
        elif data_type == DataType.metrics:
            storage_path = base_path / "5m"
        elif data_type == DataType.fundingRate:
            storage_path = base_path / "8h"
        else:
            storage_path = base_path / timeframe

        exist_files = {f.stem: f for f in storage_path.glob("*.parquet")}

        input_start = parser.parse(start).replace(tzinfo=tz.tzutc())
        input_end = parser.parse(end).replace(tzinfo=tz.tzutc())
        total_len = len(asset_info_df)

        for idx, row in (pbar := tqdm(asset_info_df.iterrows(), position=worker_id)):
            symbol = row["symbol"]
            # if symbol != "CRVUSDT":
            #     continue

            if self.is_future_type(asset_type):
                delivery_date = parser.parse(row["deliveryDate"])
                # TUSDT onboardDate 2023-01-31, but real history data is 2023-02-01
                onboard_date = parser.parse(
                    row["onboardDate"]
                )  # + dt.timedelta(days=1)
                _start = max(onboard_date, input_start)
                _end = min(delivery_date, input_end)
            else:
                _start = input_start
                _end = input_end

            if _start > _end:
                logger.warning(
                    f"{symbol} start:{_start} exceed end:{_end}, skip download."
                )
                continue

            if symbol in skip_symbols:
                logger.warning(f"{symbol} in skip_symbols, skip download.")
                continue
            if if_skip_usdc and symbol.endswith("USDC"):
                logger.warning(f"{symbol} is USDC, skip download.")
                continue

            try:
                # 增量更新逻辑：检查已有数据的最大时间
                existing_df = None
                if symbol in exist_files and not ignore_cache:
                    try:
                        existing_df = pl.read_parquet(exist_files[symbol])
                        # 根据数据类型获取时间列和最大时间
                        time_column = {
                            DataType.klines: "close_time",
                            DataType.metrics: "create_time",
                            DataType.fundingRate: "calc_time",
                        }.get(data_type, "close_time")

                        max_time = existing_df[time_column].max()
                        if max_time:
                            # 处理不同的时间类型
                            if hasattr(max_time, "to_datetime"):
                                max_time_dt = max_time.to_datetime()
                            elif hasattr(max_time, "to_python"):
                                max_time_dt = max_time.to_python()
                            else:
                                # 已经是 Python datetime
                                max_time_dt = max_time

                            # 使用已有数据的最大时间作为新的起始时间
                            _start = max(_start.replace(tzinfo=None), max_time_dt)
                            _start = _start.replace(tzinfo=tz.tzutc())

                        # 如果起始时间已经超过结束时间，跳过
                        if _start.replace(tzinfo=None) >= _end.replace(tzinfo=None):
                            logger.info(f"{symbol} already up-to-date (max: {_start}), skip.")
                            continue

                        logger.info(f"{symbol} incremental update from {_start} to {_end}")
                    except Exception as e:
                        logger.warning(f"{symbol} failed to read existing file: {e}, will re-download")
                        existing_df = None

                df = self.load_history_data(
                    symbol,
                    _start,
                    _end,
                    asset_type,
                    data_type,
                    timeframe,
                    do_filter_quote_volume_0,
                    httpx_proxies,
                )
                if data_type == DataType.klines:
                    df = df.filter(pl.col("open_time") < _end.replace(tzinfo=None))
                elif data_type == DataType.fundingRate:
                    raise ValueError("fundingRate not support filter end time")
                elif data_type == DataType.metrics:
                    df = df.filter(pl.col("create_time") < _end.replace(tzinfo=None))

                if len(df) == 0:
                    if existing_df is not None:
                        logger.info(f"{symbol} no new data, keep existing file")
                    else:
                        logger.warning(
                            f"{symbol} load df is empty, and skip save to parquet file"
                        )
                else:
                    # 如果有已有数据，合并新旧数据
                    if existing_df is not None:
                        df = pl.concat([existing_df, df])
                        # 去重并保持顺序
                        if data_type == DataType.klines:
                            df = df.unique(subset=["open_time"], maintain_order=True)
                        elif data_type == DataType.metrics:
                            df = df.unique(subset=["create_time"], maintain_order=True)
                        elif data_type == DataType.fundingRate:
                            df = df.unique(subset=["calc_time"], maintain_order=True)
                        logger.success(f"{symbol} merged, total rows: {len(df)}")

                    df.write_parquet(storage_path / f"{symbol}.parquet")
            except Exception as e:
                logger.error(f"{symbol} failed to download: {e}")
                continue

            pbar.set_description_str(
                f"[Worker_{worker_id}] Total {total_len}", refresh=False
            )
            pbar.set_postfix_str(
                f"{symbol}, download {timeframe} {asset_type.value} data from {_start} -> {_end} ..."
            )

    def load_history_data(
        self,
        symbol,
        start: datetime,
        end: datetime,
        asset_type: AssetType = AssetType.future_um,
        data_type: DataType = DataType.klines,
        timeframe: TIMEFRAMES = "1m",
        do_filter_quote_volume_0: bool = False,
        httpx_proxies: dict[str, str] = {},
    ) -> pl.DataFrame:
        load_urls, download_urls = get_urls_by_xml_parse(
            data_type=data_type,
            asset_type=asset_type,
            symbol=symbol,
            start=start,
            end=end,
            timeframe=timeframe,
            file_path=self.crypto_path.crypto,
            proxies=self._convert_proxies_for_requests(self.requests_proxies),
        )

        start_async_download_files(
            download_urls, self.crypto_path.crypto, proxies=httpx_proxies
        )
        csv_schema = self.get_csv_schema(data_type)

        df_list = []
        for url in load_urls:
            df = load_data_from_disk(
                url,
                self.crypto_path.crypto,
                dtypes=csv_schema,
            )
            if df is not None:
                df_list.append(df)
        if df_list:
            df = pl.concat(df_list)
            if data_type == DataType.klines:
                df = self.transform_klines(
                    df,
                    symbol,
                    asset_type,
                    timeframe,
                    do_filter_quote_volume_0,
                )
            elif data_type == DataType.trades:
                pass
            elif data_type == DataType.aggTrades:
                pass
            elif data_type == DataType.metrics:
                df = self.transform_metrics(df)
            elif data_type == DataType.fundingRate:
                df = self.transform_fundingRate(df, symbol)
        else:
            df = (
                pl.DataFrame({}, schema=csv_schema)
                .with_columns(jj_code=pl.lit(symbol))
                .select(pl.exclude("ignore"))
            )

        return df

    def transform_fundingRate(self, df: pl.DataFrame, symbol: str):
        return df.with_columns(
            (pl.col("calc_time") * 1e3).cast(pl.Datetime),
            jj_code=pl.lit(symbol),
        )

    def transform_metrics(self, df: pl.DataFrame):
        return df.with_columns(
            pl.col("create_time")
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            .dt.truncate("5m")
        ).rename({"symbol": "jj_code"})

    def transform_klines(
        self,
        df: pl.DataFrame,
        symbol: str,
        asset_type: AssetType,
        timeframe: TIMEFRAMES,
        do_filter_quote_volume_0: bool = False,
    ):
        from pond.duckdb.crypto.future import get_supply_df

        if do_filter_quote_volume_0:
            df = self.filter_quote_volume_0(df, symbol, timeframe)

        # spot 数据这里有timestamp不一致的问题, 这里处理
        # 新增代码：处理 open_time 和 close_time 时间戳
        df = df.with_columns(
            pl.when(pl.col("open_time").cast(str).str.len_bytes() > 13)
            .then(pl.col("open_time").cast(str).str.slice(0, 13).cast(pl.Int64))
            .otherwise(pl.col("open_time"))
            .alias("open_time"),
            pl.when(pl.col("close_time").cast(str).str.len_bytes() > 13)
            .then(pl.col("close_time").cast(str).str.slice(0, 13).cast(pl.Int64))
            .otherwise(pl.col("close_time"))
            .alias("close_time"),
        )

        df = (
            df.sort("open_time").with_columns(
                (pl.col("open_time").diff() - pl.col("open_time").diff().shift(-1))
                .fill_null(0)
                .alias("open_time_diff"),
            )
            # for debug
            # .with_columns((pl.col("open_time") * 1e3).cast(pl.Datetime))
            # .to_pandas()
        )

        lack_df = df.filter(pl.col("open_time_diff") != 0).select(
            [
                "open_time",
                "close_time",
            ]
        )
        if len(lack_df) > 0:
            if len(lack_df) % 2 != 0:
                lack_df = pl.concat(
                    [
                        lack_df,
                        df.select(["open_time", "close_time"])[-1],
                    ]
                )

            supply_df = get_supply_df(
                client=self.get_client(asset_type, self.requests_proxies),
                lack_df=lack_df,
                symbol=symbol,
                interval=timeframe,
            )
            if do_filter_quote_volume_0:
                supply_df = self.filter_quote_volume_0(supply_df, symbol, timeframe)
            df = pl.concat([df.select(pl.exclude("open_time_diff")), supply_df])
        else:
            df = df.select(pl.exclude("open_time_diff"))

        df = (
            df.select(pl.exclude("ignore"))
            .with_columns(
                (pl.col("open_time") * 1e3).cast(pl.Datetime),
                (pl.col("close_time") * 1e3).cast(pl.Datetime),
                jj_code=pl.lit(symbol),
            )
            .sort("open_time")
            .fill_null(strategy="forward")
            .unique(maintain_order=True)
        )
        # check_df = df.to_pandas()
        logger.success(
            f"[{symbol}] Dataframe shape: {df.shape}, {df['open_time'].min()} -> {df['close_time'].max()}."
        )

        return df

    def update_crypto_trades(self):
        trades_list = [f.stem for f in self.crypto_path.trades.iterdir()]
        names = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"]

        for f in (pbar := tqdm(self.crypto_path.trades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in trades_list:
                (
                    self.con.sql(
                        f"SELECT id, price, qty, quote_qty, epoch_ms(time) as time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names}) order by id"
                    ).write_parquet(
                        str(self.crypto_path.trades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )

    def update_crypto_agg_trades(self):
        agg_trades_list = [f.stem for f in self.crypto_path.aggTrades.iterdir()]
        names = [
            "agg_trade_id",
            "price",
            "qty",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ]
        for f in (pbar := tqdm(self.crypto_path.aggTrades_origin.glob("*.csv"))):
            pbar.set_postfix_str(str(f))
            if f.stem not in agg_trades_list:
                (
                    self.con.sql(
                        f"SELECT agg_trade_id, price, qty, first_trade_id, last_trade_id, "
                        f"epoch_ms(transact_time) as transact_time, is_buyer_maker  "
                        f"from read_csv_auto('{str(f)}', names={names})"
                    ).write_parquet(
                        str(self.crypto_path.aggTrades / f"{f.stem}.parquet"),
                        compression=self.compress,
                    )
                )

    def compare_um_future_info_with_vision(self):
        local_path = Path(r"E:\DuckDB\crypto\data\futures\um\monthly\klines")
        local_symbols = [i.stem for i in local_path.iterdir()]
        info_symbols = self.get_future_info(AssetType.future_um, from_local=False)[
            "symbol"
        ].to_list()
        diff_set = set(local_symbols) - set(info_symbols)
        print(diff_set)


if __name__ == "__main__":
    db = CryptoDB(
        # Path(r"/home/fangyang/DuckDB"),
        Path(r"/share/DuckDB/"),
        requests_proxies={
            "host": "127.0.0.1",
            "port": 7890,
            "protocol": "http",
        },
    )

    # db.compare_um_future_info_with_vision()

    # db = CryptoDB(Path(r"/home/fangyang/zhitai5000/DuckDB/"))

    # db.update_future_info()

    # df = db.get_future_info(asset_type=AssetType.future_um)
    # ll = db.get_local_future_perpetual_symbol_list(asset_type=AssetType.future_um)

    db.update_history_data_parallel(
        start="2020-1-1",
        end="2026-05-23",
        asset_type=AssetType.spot,
        data_type=DataType.klines,
        timeframe="1h",
        httpx_proxies={"https://": "http://127.0.0.1:7890"},
        skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT", "BTCSTUSDT"],
        do_filter_quote_volume_0=True,
        if_skip_usdc=True,
        ignore_cache=False,
        workers=os.cpu_count() - 2,
    )

    # def try_update_data(interval) -> bool:
    #     try:
    #         db.update_history_data_parallel(
    #             start="2020-1-1",
    #             end="2025-7-22",
    #             asset_type=AssetType.spot,
    #             data_type=DataType.klines,
    #             timeframe=interval,
    #             # httpx_proxies={"https://": "https://127.0.0.1:7890"},
    #             skip_symbols=["ETHBTC", "BTCDOMUSDT", "USDCUSDT"],
    #             do_filter_quote_volume_0=False,
    #             if_skip_usdc=True,
    #             ignore_cache=False,
    #             workers=os.cpu_count() - 2,
    #         )
    #     except BaseException as e:
    #         print(e)
    #         return False
    #     return True

    # ...start downloading...
    # interval = "1h"
    # complete = False
    # retry = 0
    # start_time = time.perf_counter()
    # while not complete:
    #     complete = try_update_data(interval)
    #     retry += 1
    #     if retry < 1:
    #         continue
    #     else:
    #         break
    # print(
    #     f"complete: {complete}, retried: {retry}, time cost: {time.perf_counter() - start_time:.2f}s"
    # )

    # db.update_crypto_trades()

    # for f in (pbar := tqdm(db.path_crypto_agg_trades_origin.glob("*.csv"))):
    #     r = db.con.read_csv(
    #         f,
    #         names=[
    #             "agg_trade_id",
    #             "price",
    #             "qty",
    #             "first_trade_id",
    #             "last_trade_id",
    #             "transact_time",
    #             "is_buyer_maker",
    #         ],
    #     )
    #     print(1)
    # db.update_crypto_agg_trades()
