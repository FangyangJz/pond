from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable


class StockFactor(TsTable):
    __tablename__ = "stock_factor"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    interval = Column(types.String, comment="interval")
    name = Column(types.String, comment="factor_name")
    value = Column(types.Float64, comment="value")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class CryptoFactor(TsTable):
    __tablename__ = "crypto_factor"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    interval = Column(types.String, comment="interval")
    name = Column(types.String, comment="factor_name")
    value = Column(types.Float64, comment="value")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class FuturesFactor(TsTable):
    __tablename__ = "futures_factor"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    interval = Column(types.String, comment="interval")
    name = Column(types.String, comment="factor_name")
    value = Column(types.Float64, comment="value")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )
