from datetime import datetime, timedelta
from pond.clickhouse.downoader import Downloader
import akshare as ak
import ray
from pond.utils.times import datestr
import pandas as pd
from pond.clickhouse import metadata, TsTable
from pond.clickhouse.holders import *
from typing import List
from pond.akshare.stock import get_all_stocks_df
from pond.clickhouse.kline import KlineDailyHFQ, stock_zh_a_hist
from sqlalchemy import create_engine, Column, literal, text, func, desc

from clickhouse_sqlalchemy import (
    make_session, get_declarative_base, types, engines
)

class Task:

    def __init__(self, table:TsTable, func, arg_groups) -> None:
        self.table = table
        self.func = func
        self.arg_groups = arg_groups
        self.downloaders = []
        self.distributed = True
        self.dfs = []

class ClickHouseManager:

    def __init__(self, db_uri, data_start:datetime) -> None:
        self.engine = create_engine(db_uri)
        metadata.create_all(self.engine)
        self.session = make_session(self.engine)
        self.data_start = data_start

    def sync(self, date=datetime.now()):
        print(f"click house manager syncing at {date.isoformat()}")
        tasks = self.get_syncing_tasks(date)
        max_dowloader_size = int(os.cpu_count() / len(tasks)) + 1
        for task in tasks:
            record = self.session.query(task.table).order_by(desc(task.table.datetime)).limit(1).one_or_none()
            if record is not None and record.datetime >= date:
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
                final_df["时间"] = date
                self.save_to_db(task.table, final_df)


    def save_to_db(self, table:TsTable, df:pd.DataFrame):
        df = table().format_dataframe(df)
        record = self.session.query(table).order_by(desc(table.datetime)).limit(1).one_or_none()
        if record is not None:
            df = df[df["datetime"] > record.datetime]
        df.drop_duplicates(inplace=True)
        rows = df.to_sql(table.__tablename__, self.engine, index=False, if_exists='append')
        print(f"saved {len(df)} into table {table.__tablename__}")


    def get_syncing_tasks(self, date) -> List[Task]:
        tasks:List[Task] = []
        args = {"date":datestr(date)}
        holder_types = ["个人", "基金", "QFII", "社保", "券商", "信托"]
        changements = ["新进", "增加", "不变", "减少"]
        tasks.append(Task(FreeHoldingDetail, ak.stock_gdfx_free_holding_detail_em, [args]))

        #kline daily hfq
        stock_basic = get_all_stocks_df()
        begin = self.get_cache_date(KlineDailyHFQ)
        kline_daily_args = []
        for symbol in stock_basic['代码']:
            kline_daily_args.append({"symbol":symbol, 'start_date':datestr(begin), 'end_date':datestr(date), 'period':'daily', 'adjust':'hfq'})
        tasks.append(Task(KlineDailyHFQ, stock_zh_a_hist, kline_daily_args))

        #holding detail
        holding_detail_arg_groups = []
        for holder_type in holder_types:
            for changement in changements:
                holding_detail_arg_groups.append({"date":datestr(date), "indicator":holder_type, "symbol":changement})
        tasks.append(Task(HoldingDetail, ak.stock_gdfx_holding_detail_em, holding_detail_arg_groups))
        
        #free holding statistic
        tasks.append(Task(FreeHoldingStatistic, ak.stock_gdfx_free_holding_statistics_em, [args]))

        #holding statistic
        tasks.append(Task(HoldingStatistic, ak.stock_gdfx_holding_statistics_em, [args]))

        #holder counts
        tasks.append(Task(HolderCounts, ak.stock_zh_a_gdhs, [{"symbol":datestr(date)}]))

        #restricted release detail
        begin = self.get_cache_date(StockRestrictedReleaseDetail)
        restricted_release_detail_args = {"start_date":datestr(begin), "end_date":datestr(date)}
        tasks.append(Task(StockRestrictedReleaseDetail, ak.stock_restricted_release_detail_em, [restricted_release_detail_args]))
        return tasks


    def get_cache_date(self, table:TsTable):
        record = self.session.query(table).order_by(desc(table.datetime)).limit(1).one_or_none()
        if record is not None:
            begin = record.datetime
        else:
            begin = self.data_start
        return begin

if __name__ == "__main__":
    import os
    import time
    password = os.environ.get("CLICKHOUSE_PWD")
    conn_str = f"clickhouse://default:{password}@localhost:8123/quant"
    manager = ClickHouseManager(conn_str, data_start=datetime(2018,1,1))
    begin = datetime(2024, 1, 26)
    manager.sync(date=begin)

    # end = datetime.now()
    # while begin < end:
    #     begin += timedelta(days=1)
    #     manager.sync(date=begin)
    #     time.sleep(2)
    