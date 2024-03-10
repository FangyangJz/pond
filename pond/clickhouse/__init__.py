from sqlalchemy import Column, MetaData
import pandas as pd

from clickhouse_sqlalchemy import (
    get_declarative_base,
    types,
)

metadata = MetaData()
Base = get_declarative_base(metadata=metadata)


class TsTable(Base):
    __abstract__ = True

    datetime = Column(types.DateTime64, comment="时间", primary_key=True)
    code = Column(types.String, comment="代码")

    def format_dataframe(self, df: pd.DataFrame):
        columns = {}
        for column in self.__dict__[
            "_sa_instance_state"
        ].class_.__table__.columns._all_columns:
            col: Column = column
            if col.comment in df.columns:
                columns[col.comment] = col.name
                df[col.comment] = self.format_col(col, df[col.comment])

        df = df.rename(columns=columns)
        return df[columns.values()]

    def format_col(self, col: Column, series: pd.Series):
        if isinstance(col.type, types.common.String):
            return series.astype(str)
        elif isinstance(col.type, types.common.DateTime64):
            return pd.to_datetime(series)
        elif isinstance(col.type, types.common.Int64):
            return series.astype("int")
        else:
            return series.astype("Float64")
