from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable


class FuturePosition(TsTable):
    __tablename__ = "future_position"
    datetime = Column(types.DateTime64, comment="refreshed_date", primary_key=True)
    pair = Column(types.String, comment="pair")
    strategy = Column(types.String, comment="strategy")
    account = Column(types.String, comment="account")
    strategy_id = Column(types.Int32, comment="strategy_id")
    side: str = Column(types.String, comment="side")
    stake_amount = Column(types.Float64, comment="stake_amount")
    filled_stake_amount = Column(types.Float64, comment="filled_stake_amount")
    open_price: float = Column(types.Float64, comment="open_price")
    close_reason: str = Column(types.String, comment="close_reason")
    close_price: float = Column(types.Float64, comment="close_price")
    group: int = Column(types.Int32, comment="group")
    status: str = Column(types.String, comment="status")
    open_date = Column(types.DateTime64, comment="open_date")
    refreshed_count = Column(types.Int64, comment="refreshed_count")
    prediction_refreshed_date = Column(
        types.DateTime64, comment="prediction_refreshed_date"
    )
    latest_high: float = Column(types.Float64, comment="latest_high")
    latest_low: float = Column(types.Float64, comment="latest_low")
    latest_close: float = Column(types.Float64, comment="latest_close")
    trade_id: int = Column(types.Int64, comment="trade_id")
    trade_side: str = Column(types.String, comment="trade_side")
    trade_stake_amount: float = Column(types.Float64, comment="trade_stake_amount")
    trade_leverage: float = Column(types.Float64, comment="trade_leverage")
    trade_value: float = Column(types.Float64, comment="trade_value")
    unbalance_time = Column(types.DateTime64, comment="unbalance_time")
    unbalance_state: str = Column(types.String, comment="unbalance_state")
    warn_state: str = Column(types.String, comment="warn_state")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, pair),
            primary_key=(datetime, pair),
        ),
    )


class FutureWallet(TsTable):
    __tablename__ = "future_wallet"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    machine = Column(types.String, comment="machine")
    strategy = Column(types.String, comment="strategy")
    account = Column(types.String, comment="account")
    total = Column(types.Float64, comment="total")
    free = Column(types.Float64, comment="free")
    value = Column(types.Float64, comment="value")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, strategy, account),
            primary_key=(datetime, strategy, account),
        ),
    )
