# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/9 21:22
# @Author   : Fangyang
# @Software : PyCharm

from functools import cache
from pond.akshare.stock.migrate_func import stock_zh_a_spot_em
from pond.utils.code2code import trans_to_juejin_code


@cache
def get_all_stocks_df():
    """
    tushare 的 stock_basic 包含 行业, 地域, 上市日期字段, 能区分退市股票,
    tushare pro 把这个接口的调用积分提高到2000, 导致无法调用，
    换回akshare, 考虑行业和地域信息用途不大从数据表中剔除，上市和退市日期akshare有其他接口调用
    dolphindb 中使用 tushare 接口替换这个方法
    :return:
    """
    df = stock_zh_a_spot_em()

    if "序号" in df.columns:
        df.drop("序号", inplace=True, axis=1)

    df['jj_code'] = df['代码'].apply(trans_to_juejin_code)
    return df


if __name__ == '__main__':
    import time
    # # 这里测试确实cache生效了
    start_time = time.perf_counter()
    a1 = get_all_stocks_df()
    print(f"Time cost:{time.perf_counter() - start_time:.2f}s")
    start_time = time.perf_counter()
    a2 = get_all_stocks_df()
    print(f"Time cost:{time.perf_counter()-start_time:.2f}s")
    start_time = time.perf_counter()
    a3 = get_all_stocks_df()
    print(f"Time cost:{time.perf_counter()-start_time:.2f}s")
    print(1)

