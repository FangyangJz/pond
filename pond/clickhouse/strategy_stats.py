from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable


class FuturePosition(TsTable):
    __tablename__ = "future_position"
    datetime = Column(types.DateTime64, comment="open_date", primary_key=True)
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
    refreshed_date = Column(types.DateTime64, comment="refreshed_date")

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
    strategy = Column(types.String, comment="strategy")
    account = Column(types.String, comment="account")
    total = Column(types.Float64, comment="total")
    free = Column(types.Float64, comment="free")
    used = Column(types.Float64, comment="used")

    __table_args__ = (
        engines.ReplacingMergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, strategy, account),
            primary_key=(datetime, strategy, account),
        ),
    )
