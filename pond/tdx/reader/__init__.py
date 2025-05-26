# cython: language_level=3
import struct


class TdxFileNotFoundException(Exception):
    pass


class TdxNotAssignVipdocPathException(Exception):
    pass


class Base:
    def __init__(self, vipdoc_path=None):
        """
        构造函数
        :param vipdoc_path: tdx 的 vipdoc 目录
        """
        self.vipdoc_path = vipdoc_path

    @staticmethod
    def unpack_records(fmt, data):
        """
        解包
        :param fmt:
        :param data:
        :return:
        """
        record = struct.Struct(fmt)
        result = (record.unpack_from(data, offset) for offset in range(0, len(data), record.size))

        return result

    def get_df(self, code_or_file, exchange=None):
        """
        转换 pd.DataFrame
        :param code_or_file:
        :param exchange:
        """
        raise NotImplementedError("not yet")

    @staticmethod
    def _parse_date(num):
        """
        解析日期
        :param num:
        :return:
        """
        month = (num % 2048) // 100
        year = num // 2048 + 2004
        day = (num % 2048) % 100

        return year, month, day

    @staticmethod
    def _parse_time(num):
        """
        解析时间
        :param num:
        :return:
        """
        return (num // 60), (num % 60)
