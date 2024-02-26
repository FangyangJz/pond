# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/18 上午 08:04
# @Author   : Fangyang
# @Software : PyCharm

import time
from loguru import logger
from functools import wraps
from datetime import datetime


def datestr(date: datetime, spliter=""):
    return date.date().isoformat().replace("-", spliter)


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
