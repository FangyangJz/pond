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


class StockKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "stock_kline_5m"

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
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BaoStockKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "baostock_kline_5m"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BaoStockKline1dHfq(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "baostock_kline_1d_hfq"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")
    turn = Column(types.Float64, comment="turn")
    peTTM = Column(types.Float64, comment="peTTM")
    psTTM = Column(types.Float64, comment="psTTM")
    pcfNcfTTM = Column(types.Float64, comment="pcfNcfTTM")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYear(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class BondKline5m(TsTable):
    """
    5m K线，不复权
    """

    __tablename__ = "bond_kline_5m"

    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    close_time = Column(types.DateTime64, comment="close_time")
    code = Column(types.String, comment="code")
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    amount = Column(types.Float64, comment="amount")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(open_time),
            order_by=(open_time, code),
            primary_key=(open_time, code),
        ),
    )


class StockKline15m(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "stock_kline_15m"

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
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class StockKlineHFQ15M(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "stock_kline_hfq_15m"

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
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class KlineDailyNFQ(TsTable):
    """
    日K线，后复权
    """

    __tablename__ = "kline_daily_nfq"

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


class FutureInfo(TsTable):
    """
    合约信息表
    """

    __tablename__ = "future_info"

    datetime = Column(types.DateTime64, comment="datetime", primary_key=True)
    code = Column(types.String, comment="jj_code")
    total_supply = Column(types.Float64, comment="total_supply")
    market_cap_fdv_ratio = Column(types.Float64, comment="market_cap_fdv_ratio")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class TokenHolders(TsTable):
    """
    合约信息表
    """

    __tablename__ = "token_holders"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    chain = Column(types.String, comment="chain")
    wallet_address = Column(types.String, comment="wallet_address")
    amount = Column(types.Float64, comment="amount")
    usd_value = Column(types.Float64, comment="usd_value")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code, chain, wallet_address),
            primary_key=(datetime, code, chain, wallet_address),
        ),
    )


class FutureLongShortRatio(TsTable):
    """
    合约多空账户数比信息表

    按保证金余额（账户资金规模）排名前 20% 的用户（即 “大户”），仅统计有未平仓合约的账户。

    longAccount: 有净多头头寸的大户账户数 ÷ 有未平仓头寸的大户总账户数
    shortAccount: 有净空头头寸的大户账户数 ÷ 有未平仓头寸的大户总账户数
    longShortRatio: 多空账户数比
    """

    __tablename__ = "future_long_short_ratio"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    longAccount = Column(types.Float64, comment="longAccount")
    shortAccount = Column(types.Float64, comment="shortAccount")
    longShortRatio = Column(types.Float64, comment="longShortRatio")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureOpenInterest(TsTable):
    """
    合约持仓信息表
    sumOpenInterest: 总持仓量
    sumOpenInterestValue: 总持仓价值
    CMCCirculatingSupply: CMC 统计的总流通量
        总流通量: 排除项目方未解锁、团队锁仓、基金会储备等 “暂不可交易” 的部分，仅统计当前能在交易所、钱包间自由流通的代币数量
        数据来源：由 CMC 统计并提供，币安 API 仅做同步展示，其统计逻辑可能与项目方官方数据略有差异（如是否计入跨链流通的代币、解锁进度更新时效等）。
        非实时更新：CMC 流通供应量通常每日更新一次，并非实时变动，因此不适合作为实时交易决策的直接依据，仅用于中长期趋势参考。
    """

    __tablename__ = "future_open_interest"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    sumOpenInterest = Column(types.Float64, comment="sumOpenInterest")
    sumOpenInterestValue = Column(types.Float64, comment="sumOpenInterestValue")
    CMCCirculatingSupply = Column(types.Float64, comment="CMCCirculatingSupply")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FutureFundingRate(TsTable):
    """
    合约资金费率表
    """

    __tablename__ = "future_funding_rate"

    datetime = Column(types.DateTime64, comment="fundingTime", primary_key=True)
    code = Column(types.String, comment="symbol")
    fundingRate = Column(types.Float64, comment="fundingRate")
    markPrice = Column(types.Float64, comment="markPrice")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline1H(TsTable):
    __tablename__ = "kline_futures_1h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class SpotKline1H(TsTable):
    __tablename__ = "kline_spot_1h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline4H(TsTable):
    __tablename__ = "kline_futures_4h"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline15m(TsTable):
    __tablename__ = "kline_futures_15m"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline5m(TsTable):
    __tablename__ = "kline_futures_5m"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesKline1d(TsTable):
    __tablename__ = "kline_futures_1d"

    code = Column(types.String, comment="jj_code")
    open_time = Column(types.DateTime64, comment="open_time", primary_key=True)
    open = Column(types.Float64, comment="open")
    high = Column(types.Float64, comment="high")
    low = Column(types.Float64, comment="low")
    close = Column(types.Float64, comment="close")
    volume = Column(types.Float64, comment="volume")
    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    quote_volume = Column(types.Float64, comment="quote_volume")
    count = Column(types.Float64, comment="count")
    taker_buy_volume = Column(types.Float64, comment="taker_buy_volume")
    taker_buy_quote_volume = Column(types.Float64, comment="taker_buy_quote_volume")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FundKlineDailyHFQ(TsTable):
    """
    基金日K线，后复权
    """

    __tablename__ = "fund_kline_daily_hfq"

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
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FundNetValue(TsTable):
    """
    基金净值数据
    """

    __tablename__ = "fund_value"

    datetime = Column(types.DateTime64, comment="净值日期", primary_key=True)
    code = Column(types.String, comment="代码")
    net_value = Column(types.Float64, comment="单位净值")
    cumsum_value = Column(types.Float64, comment="累计净值")
    tradable = Column(types.Boolean, comment="tradable")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )
