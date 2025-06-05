import os
import math
import datetime as dtm
from typing import Any

import ray
import pandas as pd
import polars as pl
import akshare as ak
from loguru import logger
import clickhouse_connect
from clickhouse_driver import Client
from urllib.parse import urlparse
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, desc
from deprecated import deprecated

from pond.clickhouse.downoader import Downloader
from pond.clickhouse.holders import (
    FreeHoldingDetail,
    FreeHoldingStatistic,
    HolderCounts,
    HoldingDetail,
    HoldingStatistic,
    StockRestrictedReleaseDetail,
)
from pond.utils.times import datestr, timeit_cls_method_wrapper
from pond.clickhouse import metadata, TsTable
from pond.akshare.stock import get_all_stocks_df
from pond.clickhouse.kline import KlineDailyNFQ, stock_zh_a_hist


class Task:
    def __init__(self, table: TsTable, func, arg_groups, distributed=False) -> None:
        self.table = table
        self.func = func
        self.arg_groups = arg_groups
        self.downloaders = []
        self.distributed = distributed
        self.dfs = []


class ClickHouseManager:
    def __init__(
        self,
        db_uri: str,
        data_start: dtm.datetime = None,
        native_uri: str | None = None,
    ) -> None:
        self.engine = create_engine(db_uri)
        metadata.create_all(self.engine)
        self.data_start = data_start
        self.native_uri = native_uri

    def get_engine(self):
        return self.engine

    def create_client(self, db_uri: str):
        parts = urlparse(db_uri)
        configs = {
            "host": parts.hostname,
            "port": parts.port if parts.port else 9000,
            "user": parts.username,
            "password": parts.password,
            "connect_timeout": 15,
            "database": parts.path[1:],
            "settings": {"distributed_ddl_task_timeout": 300},
        }
        return clickhouse_connect.get_client(**configs)

    def sync(self, tasks: list[Task] = [], end: dtm.datetime | None = None):
        logger.info(
            f"clickhouse manager syncing at {end.isoformat()}, task size {len(tasks)}"
        )
        max_dowloader_size = int(os.cpu_count() / len(tasks)) + 1
        for task in tasks:
            latest_record_time = self.get_latest_record_time(task.table)
            if latest_record_time >= end:
                continue
            for i in range(len(task.arg_groups)):
                kwargs = task.arg_groups[i]
                if task.distributed:
                    if len(task.downloaders) < max_dowloader_size:
                        downloader = Downloader.remote()
                        task.downloaders.append(downloader)
                    else:
                        downloader = task.downloaders[i % max_dowloader_size]
                    downloader.download.remote(task.func, **kwargs)
                else:
                    df = task.func(**kwargs)
                    if df is not None and len(df) > 0:
                        task.dfs.append(df)

        for task in tasks:
            dfs = task.dfs
            for downloader in task.downloaders:
                df = ray.get(downloader.get.remote())
                if df is not None and len(df) > 0:
                    dfs.append(df)
            if len(dfs) > 0:
                final_df = pd.concat(dfs)
                self.save_to_db(task.table, final_df)

    def __native_read_table(
        self,
        table: str | TsTable,
        start_date: dtm.datetime,
        end_date: dtm.datetime,
        filters: list[str] | None = None,
        params: dict[str, Any] | None = None,
        rename: bool = False,
        datetime_col: str = "datetime",
        columns: list[str] = [],
    ) -> pd.DataFrame:
        if isinstance(table, str):
            table_name = table
        else:
            table_name = table.__tablename__
        if len(columns) > 0:
            columns = ",".join(columns)
        else:
            columns = "*"
        sql = f"select {columns} from {table_name}"
        query_params = {}
        if start_date is None:
            start_date = self.data_start
        sql += f" where {datetime_col} > %(start)s "
        query_params["start"] = start_date
        if end_date is not None:
            sql += f" AND {datetime_col} <= %(end)s "
            query_params["end"] = end_date
        if filters is not None:
            if not isinstance(filters, list):
                filters = [filters]
            for filter in filters:
                sql += f" {filter} "
        if params is not None:
            query_params.update(params)

        with Client.from_url(self.native_uri) as client:
            df = client.query_dataframe(
                query=sql, params=query_params, settings=dict(use_numpy=True)
            )
            if rename and not isinstance(table, str):
                df = df.rename(columns=table().get_colcom_names())
            return df

    def native_read_table(
        self,
        table: str | TsTable,
        start_date: dtm.datetime,
        end_date: dtm.datetime,
        filters: list[str] | None = None,
        params=None,
        rename: bool = False,
        datetime_col: str = "datetime",
        trunk_days: int = None,
        columns=[],
    ) -> pd.DataFrame:
        # 解决时间左开问题
        if start_date is not None:
            start_date -= dtm.timedelta(seconds=1)
        if trunk_days is None:
            starts = [start_date]
            ends = [end_date]
        else:
            dt_splits = math.ceil(
                (end_date - start_date).total_seconds() / 3600 / 24 / trunk_days
            )
            dt_splits = max(1, dt_splits)
            dt_step = (end_date - start_date) // dt_splits
            starts = [start_date + dt_step * i for i in range(0, dt_splits)]
            ends = [start_date + dt_step * (i + 1) for i in range(0, dt_splits)]
        dfs = []
        for start, end in zip(starts, ends):
            logger.info(f"reading {table} from {start} to {end}")
            df = self.__native_read_table(
                table, start, end, filters, params, rename, datetime_col, columns
            )
            if df is not None and len(df) > 0:
                dfs.append(df)
        if len(dfs) > 0:
            return pd.concat(dfs)
        return None

    @deprecated("this method works quite slowly, use native read table instead.")
    def read_table(
        self,
        table: TsTable,
        start_date: dtm.datetime,
        end_date: dtm.datetime,
        filters: list[str] | None = None,
        rename=False,
    ) -> pl.DataFrame:
        with Session(self.engine) as session:
            query = session.query(table)
            if start_date is not None:
                query = query.filter(table.datetime >= start_date)
            if end_date is not None:
                query = query.filter(table.datetime <= end_date)
            if filters is not None:
                if not isinstance(filters, list):
                    filters = [filters]
                for f in filters:
                    query = query.filter(f)
            df = pl.read_database(query.statement, session.connection())
            if rename:
                df = df.rename(table().get_colcom_names())
        return df

    @timeit_cls_method_wrapper
    def save_to_db(
        self,
        table: str | TsTable,
        df: pd.DataFrame,
        last_record_filters: list[str] | None,
        datetime_col="datetime",
    ) -> int:
        # format data
        if isinstance(table, str):
            table_name = table
            lastet_record_time = self.native_get_latest_record_time(
                table, last_record_filters, datetime_col
            )
        else:
            table_name = table.__tablename__
            df = table().format_dataframe(df)
            if "datetime" in df.columns and "code" in df.columns:
                df.drop_duplicates(subset=["datetime", "code"], inplace=True)
            lastet_record_time = self.get_latest_record_time(table, last_record_filters)

        origin_len = len(df)
        if lastet_record_time is not None:
            tz = None
            try:
                tz = getattr(df.dtypes[datetime_col], "tz")
            except Exception:
                pass
            if tz is not None and lastet_record_time.tzinfo is not None:
                lastet_record_time = lastet_record_time.astimezone(tz)
            elif tz is not None and lastet_record_time.tzinfo is None:
                lastet_record_time = lastet_record_time.replace(
                    tzinfo=dtm.timezone.utc
                ).astimezone(tz)
            elif tz is None and lastet_record_time.tzinfo is not None:
                lastet_record_time = lastet_record_time.astimezone(
                    dtm.timezone.utc
                ).replace(tzinfo=None)

            df[datetime_col] = df[datetime_col].dt.floor(freq="1s")
            df = df[df[datetime_col] > lastet_record_time]
            # df = df[df["datetime"] > lastet_record_time.replace(tzinfo=df.dtypes['datetime'].tz)]
        if len(df) == 0:
            logger.info(
                f"dataframe is empty after filter by latest record, original len {origin_len}"
            )
            return 0
        query = f"INSERT INTO {table_name} (*) VALUES"
        with Client.from_url(self.native_uri) as client:
            rows = client.insert_dataframe(
                query=query, dataframe=df, settings=dict(use_numpy=True)
            )
            logger.success(
                f"total {len(df)} saved {rows} into table {table_name}, latest record time {lastet_record_time}"
            )
            return rows

    def get_syncing_tasks(self, date: dtm.datetime) -> list[Task]:
        tasks: list[Task] = []
        args = {"date": datestr(date)}

        # kline daily hfq
        stock_basic = get_all_stocks_df()
        begin = self.get_latest_record_time(KlineDailyNFQ)
        kline_nfq_daily_args = []
        for symbol in stock_basic["代码"]:
            kline_nfq_daily_args.append(
                {
                    "symbol": symbol,
                    "start_date": datestr(begin),
                    "end_date": datestr(date),
                    "period": "daily",
                    "adjust": "",
                }
            )
        tasks.append(Task(KlineDailyNFQ, stock_zh_a_hist, kline_nfq_daily_args))
        return tasks

        # free hoding detail
        tasks.append(
            Task(FreeHoldingDetail, ak.stock_gdfx_free_holding_detail_em, [args])
        )

        # holding detail
        holder_types = ["个人", "基金", "QFII", "社保", "券商", "信托"]
        changements = ["新进", "增加", "不变", "减少"]
        holding_detail_arg_groups = []
        for holder_type in holder_types:
            for changement in changements:
                holding_detail_arg_groups.append(
                    {
                        "date": datestr(date),
                        "indicator": holder_type,
                        "symbol": changement,
                    }
                )
        tasks.append(
            Task(
                HoldingDetail,
                ak.stock_gdfx_holding_detail_em,
                holding_detail_arg_groups,
            )
        )

        # free holding statistic
        tasks.append(
            Task(FreeHoldingStatistic, ak.stock_gdfx_free_holding_statistics_em, [args])
        )

        # holding statistic
        tasks.append(
            Task(HoldingStatistic, ak.stock_gdfx_holding_statistics_em, [args])
        )

        # holder counts
        tasks.append(
            Task(HolderCounts, ak.stock_zh_a_gdhs, [{"symbol": datestr(date)}])
        )

        # restricted release detail
        begin = self.get_latest_record_time(StockRestrictedReleaseDetail)
        restricted_release_detail_args = {
            "start_date": datestr(begin),
            "end_date": datestr(date),
        }
        tasks.append(
            Task(
                StockRestrictedReleaseDetail,
                ak.stock_restricted_release_detail_em,
                [restricted_release_detail_args],
            )
        )
        return tasks

    def get_latest_record(
        self, table: TsTable, filters: list[str] | None = None
    ) -> dtm.datetime:
        with Session(self.engine) as session:
            query = session.query(table)
            if filters is not None:
                if not isinstance(filters, list):
                    filters = [filters]
                for f in filters:
                    query = query.filter(f)
            return query.order_by(desc(table.datetime)).limit(1).one_or_none()

    def get_latest_record_time(
        self, table: TsTable, filters: list[str] | None = None
    ) -> dtm.datetime:
        record = self.get_latest_record(table, filters)
        if record is not None:
            begin = record.datetime
        else:
            begin = self.data_start
        return begin

    def native_get_latest_record_time(
        self, table: TsTable, filters: str | list[str], datetime_col: str = "datetime"
    ) -> dtm.datetime:
        if filters is None:
            filters = []
        elif isinstance(filters, str):
            filters = [filters]
        filters.append(f"ORDER BY {datetime_col} DESC LIMIT 1")
        try:
            df = self.native_read_table(
                table, None, None, filters, datetime_col=datetime_col
            )
            if len(df) > 0:
                return df[datetime_col][0]
        except Exception:
            pass
        return self.data_start

    def create_table(self, table_name: str, order_by_cols: list[str], df: pl.DataFrame):
        columns_ddl = ""
        for i in range(len(df.columns)):
            col = df.columns[i]
            dtype = df.dtypes[i]
            if str(dtype).lower().startswith("datetime"):
                columns_ddl += f"{col} Datetime,"
            elif str(dtype).lower().startswith("str"):
                columns_ddl += f"{col} String,"
            elif str(dtype).lower().startswith("int"):
                columns_ddl += f"{col} Int,"
            elif str(dtype).lower().startswith("float"):
                columns_ddl += f"{col} Float64,"
            else:
                raise (f"unsupport dtype for {col} {dtype}")
        # remove last comma.
        columns_ddl = columns_ddl[:-1]
        orderby = ",".join(order_by_cols)

        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ( {columns_ddl} ) ENGINE = ReplacingMergeTree ORDER BY ({orderby})"
        with Client.from_url(self.native_uri) as client:
            client.execute(ddl)


if __name__ == "__main__":
    import os

    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    native_conn_str = f"clickhouse+native://default:{password}@localhost:9000/quant?tcp_keepalive=true"
    manager = ClickHouseManager(
        conn_str, data_start=dtm.datetime(2020, 1, 1), native_uri=native_conn_str
    )
    begin = dtm.datetime(2021, 1, 1)
    manager.sync()

    # end = datetime.now()
    # while begin < end:
    #     begin += timedelta(days=1)
    #     manager.sync(date=begin)
    #     time.sleep(2)
