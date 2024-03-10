from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable

import akshare as ak


def stock_zh_a_hist(**kwargs):
    print(f"stock_zh_a_hist {kwargs}")
    df = ak.stock_zh_a_hist(**kwargs)
    if df is not None:
        df["代码"] = kwargs["symbol"]
        print(f"stock_zh_a_hist {kwargs['symbol']}, size {len(df)}")
    return df


class KlineDailyHFQ(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "kline_daily_hfq"

    datetime = Column(types.DateTime64, comment="日期", primary_key=True)
    code = Column(types.String, comment="代码")
    open = Column(types.Float64, comment="开盘")
    high = Column(types.Float64, comment="最高")
    low = Column(types.Float64, comment="最低")
    close = Column(types.Float64, comment="收盘")
    volume = Column(types.Float64, comment="成交量")
    amount = Column(types.Float64, comment="成交额")
    turn = Column(types.Float64, comment="换手率")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )
