from datetime import datetime

import pandas as pd
from pond.clickhouse import TsTable
from pond.enums import Adjust, Interval, Product


class DataProxy:
    def get_table(
        self, interval: Interval, adjust: Adjust, product: Product = Product.STOCK
    ) -> TsTable | None:
        pass

    def get_symobls(self) -> list[str]:
        pass

    def get_klines(
        self,
        symbol: str,
        period: Interval,
        adjust: Adjust,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> pd.DataFrame:
        pass
