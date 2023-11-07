# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 21:26
# @Author   : Fangyang
# @Software : PyCharm

from pond.utils.stock import MARKET_BJ, MARKET_SH, MARKET_SZ, get_stock_market

JUEJIN_MARKET_MAP = {
    MARKET_SH: "SHSE",
    MARKET_SZ: "SZSE",
    MARKET_BJ: "BJSE"
}


def trans_to_juejin_code(code):
    return f"{JUEJIN_MARKET_MAP[get_stock_market(code)]}.{code}"


if __name__ == '__main__':
    pass
