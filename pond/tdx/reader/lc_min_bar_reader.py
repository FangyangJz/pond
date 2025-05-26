import datetime as dt
from collections import OrderedDict
from pathlib import Path

from loguru import logger
import polars as pl

from pond.enums import Interval
from pond.tdx.reader import Base
from pond.utils.stock import get_stock_market


# 网传秘籍...
# 二、通达信5分钟线*.lc5文件和*.lc1文件 文件名即股票代码 每32个字节为一个5分钟数据，每字段内低字节在前 00 ~ 01 字节：日期，整型，
# 设其值为num，则日期计算方法为： year=floor(num/2048)+2004; month=floor(mod(num,2048)/100); day=mod(mod(num,2048),100);
# 02 ~ 03 字节： 从0点开始至目前的分钟数，整型 04 ~ 07 字节：开盘价，float型 08 ~ 11 字节：最高价，float型 12 ~ 15 字节：最低价，
# float型 16 ~ 19 字节：收盘价，float型 20 ~ 23 字节：成交额，float型 24 ~ 27 字节：成交量（股），整型 28 ~ 31 字节：（保留）


class TdxLCMinBarReader(Base):
    """
    读取通达信分钟数据
    """

    def __init__(self, tdx_path: Path, interval: Interval):
        super().__init__(vipdoc_path=tdx_path / "vipdoc")
        self.tdx_path = tdx_path
        self.interval = interval
        self.file_suffix = {"5m": "lc5", "1m": "lc1"}[interval]

    def parse_data_by_file(self, filename):
        """

        :param filename:
        :return:
        """
        data = []

        if not Path(filename).is_file():
            logger.error(f"no tdx kline data, please check path {filename}")
            return data

        content = Path(filename).read_bytes()
        raw_li = self.unpack_records("<HHfffffII", content)  # noqa

        for row in raw_li:
            year, month, day = self._parse_date(row[0])
            hour, minute = self._parse_time(row[1])
            close_time = dt.datetime(year, month, day, hour, minute)
            open_time = close_time - self.interval.timedelta
            data.append(
                OrderedDict([
                    ("open_time", open_time),
                    ("close_time", close_time),
                    ("open", row[2]),
                    ("high", row[3]),
                    ("low", row[4]),
                    ("close", row[5]),
                    ("amount", row[6]),
                    ("volume", row[7]),
                    # ('unknown', row[8])
                ])
            )

        return data

    def get_df(self, code: str, **kwargs) -> pl.DataFrame | None:
        """
        :param code_or_file:
        :param kwargs:
        :return:
        """
        mk = get_stock_market(code, string=True)
        file_path = self.vipdoc_path / mk / "fzline" / f"{mk}{code}.{self.file_suffix}"
        data = self.parse_data_by_file(file_path)

        if data:
            df = pl.from_records(data).with_columns(code=pl.lit(code))
            return df
        else:
            return None


if __name__ == "__main__":
    from pond.akshare.bond.all_basic import get_bond_basic_df

    df1, df2 = get_bond_basic_df()

    df = TdxLCMinBarReader(
        interval=Interval("5m"), tdx_path=Path(r"e:\new_tdx")
    ).get_df(code="113637")
    print(df)
