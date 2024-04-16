# !/usr/bin/env python3
# @Datetime : 2023/12/18 上午 08:04
# @Author   : Fangyang
# @Software : PyCharm

import time
from loguru import logger
from functools import wraps
from datetime import datetime
import datetime as dtm


def remove_tz(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        dt = dt.astimezone(tz=dtm.timezone.utc).replace(tzinfo=None)
    return dt


def datetime2utctimestamp_milli(datetime: datetime) -> int:
    if datetime.tzinfo is not None:
        datetime = datetime.astimezone(dtm.timezone.utc)
    return int(datetime.timestamp() * 1e3)


def utcstamp_mill2datetime(stamp: int):
    return datetime.fromtimestamp(stamp / 1e3).astimezone(dtm.timezone.utc)


def utc_now_stamp() -> int:
    return int(datetime.now(tz=dtm.timezone.utc).timestamp() * 1e3)


def datestr(date: datetime, spliter=""):
    return date.date().isoformat().replace("-", spliter)


def timeframe2minutes(tf: str):
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 60 * 24


def timeit_function_wrapper(func):
    @wraps(func)  # --> 4
    def clocked(*args, **kwargs):  # -- 1
        """this is inner clocked function"""
        start_time = time.time()
        result = func(*args, **kwargs)  # --> 2
        logger.success(
            f"Function {func.__name__} execute cost time : {time.time() - start_time:.3f}s"
        )
        return result

    return clocked  # --> 3


def timeit_cls_method_wrapper(func):
    @wraps(func)  # --> 4
    def clocked(self, *args, **kwargs):  # -- 1
        """this is inner clocked function"""
        start_time = time.time()
        result = func(self, *args, **kwargs)  # --> 2
        logger.success(
            f"Class method {func.__name__} execute cost time : {time.time() - start_time:.2f}s"
        )
        return result

    return clocked  # --> 3


if __name__ == "__main__":
    pass
