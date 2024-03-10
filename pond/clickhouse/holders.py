from sqlalchemy import create_engine, Column, MetaData, func
from clickhouse_sqlalchemy import (
    Table,
    make_session,
    get_declarative_base,
    types,
    engines,
)
from pond.clickhouse import TsTable, Base


class FreeHoldingDetail(TsTable):
    """
    持股明细-十大流通股东
    """

    __tablename__ = "FreeHoldingDetail"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    volume = Column(types.Float64, comment="期末持股-数量")
    free_amount = Column(types.Float64, comment="期末持股-流通市值")
    volume_change = Column(types.Float64, comment="期末持股-数量变化")
    volume_change_percent = Column(types.Float64, comment="期末持股-数量变化比例")
    report_date = Column(types.DateTime64, comment="报告期")
    publish_date = Column(types.DateTime64, comment="公告日")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class HoldingDetail(TsTable):

    __tablename__ = "HoldingDetail"

    """
    持股明细-十大股东
    """
    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    volume = Column(types.Float64, comment="期末持股-数量")
    amount = Column(types.Float64, comment="期末持股-流通市值")
    volume_change = Column(types.Float64, comment="期末持股-数量变化")
    volume_change_percent = Column(types.Float64, comment="期末持股-数量变化比例")
    holder_rank = Column(types.Float64, comment="股东排名")
    report_date = Column(types.DateTime64, comment="报告期")
    publish_date = Column(types.DateTime64, comment="公告日")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FreeHoldingStatistic(TsTable):
    """
    持股统计-十大流通股东
    """

    __tablename__ = "FreeHoldingStatistic"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    stat_counts = Column(types.Float64, comment="统计次数")
    avg_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-平均涨幅")
    max_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-最大涨幅")
    min_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-最小涨幅")
    avg_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-平均涨幅")
    max_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-最大涨幅")
    min_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-最小涨幅")
    avg_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-平均涨幅")
    max_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-最大涨幅")
    min_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-最小涨幅")
    holding_codes = Column(types.String, comment="持有个股")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime),
            primary_key=(datetime),
        ),
    )


class HoldingStatistic(TsTable):
    """
    持股统计-十大股东
    """

    __tablename__ = "HoldingStatistic"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="股票代码")
    name = Column(types.String, comment="股东名称")
    nature = Column(types.String, comment="股东类型")
    stat_counts = Column(types.Float64, comment="统计次数")
    avg_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-平均涨幅")
    max_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-最大涨幅")
    min_chg_pct_day10 = Column(types.Float64, comment="公告日后涨幅统计-10个交易日-最小涨幅")
    avg_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-平均涨幅")
    max_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-最大涨幅")
    min_chg_pct_day30 = Column(types.Float64, comment="公告日后涨幅统计-30个交易日-最小涨幅")
    avg_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-平均涨幅")
    max_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-最大涨幅")
    min_chg_pct_day60 = Column(types.Float64, comment="公告日后涨幅统计-60个交易日-最小涨幅")
    holding_codes = Column(types.String, comment="持有个股")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class HolderCounts(TsTable):

    """
    股东户数
    """

    __tablename__ = "HolderCounts"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="代码")
    report_date = Column(types.DateTime64, comment="股东户数统计截止日-本次")
    last_report_date = Column(types.DateTime64, comment="股东户数统计截止日-上次")
    period_chg_pct = Column(types.Float64, comment="区间涨跌幅")
    counts = Column(types.Float64, comment="股东户数-本次")
    last_counts = Column(types.Float64, comment="股东户数-上次")
    counts_change = Column(types.Float64, comment="股东户数-增减")
    counts_change_percent = Column(types.Float64, comment="股东户数-增减比例")
    amount_avg = Column(types.Float64, comment="户均持股市值")
    volume_avg = Column(types.Float64, comment="户均持股数量")
    amount_toal = Column(types.Float64, comment="总市值")
    volume_total = Column(types.Float64, comment="总股本")
    publish_date = Column(types.DateTime64, comment="公告日期")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class StockRestrictedReleaseDetail(TsTable):

    __tablename__ = "StockRestrictedReleaseDetail"

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="股票代码")
    release_date = Column(types.DateTime64, comment="解禁时间")
    restriction = Column(types.String, comment="限售股类型")
    release_volume = Column(types.Float64, comment="解禁数量")
    release_percent = Column(types.Float64, comment="占解禁前流通市值比例")
    release_volume_actual = Column(types.Float64, comment="实际解禁数量")
    release_amount_actual = Column(types.Float64, comment="实际解禁市值")
    chg_pct_before_day20 = Column(types.Float64, comment="解禁前20日涨跌幅")
    chg_pct_afer_day20 = Column(types.Float64, comment="解禁后20日涨跌幅")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )
