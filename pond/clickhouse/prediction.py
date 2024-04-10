from sqlalchemy import Column, func
from clickhouse_sqlalchemy import types, engines
from pond.clickhouse import TsTable

class StockPrediction(TsTable):

    __tablename__ = "stock_prediction"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    model = Column(types.String, comment="model")
    value = Column(types.Float64, comment="y_pred")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )


class CryptoPrediction(TsTable):

    __tablename__ = "crypto_prediction"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    model = Column(types.String, comment="model")
    value = Column(types.Float64, comment="y_pred")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )

class FuturesPrediction(TsTable):

    __tablename__ = "futures_prediction"

    datetime = Column(types.DateTime64, comment="close_time", primary_key=True)
    code = Column(types.String, comment="jj_code")
    model = Column(types.String, comment="model")
    value = Column(types.Float64, comment="y_pred")

    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(datetime),
            order_by=(datetime, code),
            primary_key=(datetime, code),
        ),
    )