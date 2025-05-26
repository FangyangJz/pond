from datetime import timedelta
from enum import StrEnum


class Interval(StrEnum):
    """
    时间间隔
    """

    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

    @property
    def minutes(self):
        if self.value.endswith("m"):
            return int(self.value[:-1])
        elif self.value.endswith("h"):
            return int(self.value[:-1]) * 60
        elif self.value.endswith("d"):
            return int(self.value[:-1]) * 60 * 24
        elif self.value.endswith("w"):
            return int(self.value[:-1]) * 60 * 24 * 7
        elif self.value.endswith("M"):
            # 粗略估算一个月为 30 天
            return int(self.value[:-1]) * 60 * 24 * 30
        return 0

    @property
    def seconds(self):
        return self.minutes * 60

    @property
    def timedelta(self) -> timedelta:
        if self.value.endswith("m"):
            return timedelta(minutes=int(self.value[:-1]))
        elif self.value.endswith("h"):
            return timedelta(hours=int(self.value[:-1]))
        elif self.value.endswith("d"):
            return timedelta(days=int(self.value[:-1]))
        elif self.value.endswith("w"):
            return timedelta(weeks=int(self.value[:-1]))
        elif self.value.endswith("M"):
            # 粗略估算一个月为 30 天
            return timedelta(days=int(self.value[:-1]) * 30)


class Adjust(StrEnum):
    """
    复权类型
    """

    NFQ = "3"  # 不复权
    QFQ = "2"  # 前复权
    HFQ = "1"  # 后复权


class Product(StrEnum):
    """
    Product class.
    """

    STOCK = "股票"
    FUTURES = "期货"
    OPTION = "期权"
    INDEX = "指数"
    FOREX = "外汇"
    SPOT = "现货"
    ETF = "ETF"
    BOND = "债券"
    WARRANT = "权证"
    SPREAD = "价差"
    FUND = "基金"
    CFD = "CFD"
    SWAP = "互换"


if __name__ == "__main__":
    print(Interval.MINUTE_1)
    print(Interval.MINUTE_1.value)
    print(Interval.MINUTE_1 == "1m")

    print(Product.STOCK)

    print(Interval.MINUTE_1.timedelta)
    print(Interval.MINUTE_5.timedelta)
    print(Interval.MINUTE_15.timedelta)
    print(Interval.HOUR_1.timedelta)
    print(Interval.DAY_1.timedelta)
