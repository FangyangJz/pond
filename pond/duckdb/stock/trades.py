# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/11/7 23:01
# @Author   : Fangyang
# @Software : PyCharm
import gc
import os
import time
from typing import List, Callable

import pandas as pd
# import polars as pl
from multiprocess.managers import ListProxy
from tqdm import tqdm
from pathlib import Path
from multiprocessing import Pool, Manager
from loguru import logger


def get_trade_df(csv_file: Path) -> pd.DataFrame:
    """
    From 海通证券 选股因子系列研究75 限价订单簿LOB的还原与应用

    交易所的 Level2 行情采样于交易所核心交易系统中的交易信息，快照行情与逐笔行情拥有不同的时间粒度，
    两者可能由不同的采样程序负责采集并生成。逐笔委托与逐笔成交拥有统一序号，两者应由同一采样程序处理生成。
    虽然逐笔数据的最小时间单位为 0.01 秒，但在 0.01 秒中可能包含几十甚至上百条逐笔信息。
    因此，可利用逐笔行情的统一编号将每个 0.01 秒中的逐笔委托与逐笔成交信息排序，
    从而还原出成交、委托行为的先后顺序，保证后续盘口还原等操作的顺利完成。

    需要注意的是，沪深交易所的逐笔行情存在较为显著的差异。实时数据的推送方式上，
    深交所理论上每 0.01 秒就会推送最新的逐笔数据，而上交所则是将过去 3 秒内所有的逐笔数据与快照数据一起打包后推送。

    数据结构差异:
    1. 深交所撤单信息保存于逐笔成交行情中，每个逐笔成交均标有“成交”与“撤单”两种执行类型中的一种。
    上交所撤单信息包含在逐笔委托行情中，有专门的“删除委托订单”字段;
    2. 深交所逐笔委托中包含对手方最优与本手方最优两种市价单类型，但订单价格显示为 0 或 -1。
    因此，这些订单的实际委托价格需要利用前序逐笔信息计算;
    3. 最后，深交所逐笔成交行情涉及的所有订单均可在逐笔委托行情中找到对应的委托信息；
    而上交所的逐笔行情中，若某订单全部成交，则逐笔委托行情中无该订单信息。
    """
    dtype_dict = {
        '万得代码': str,  # keep convert to jj_code
        # '交易所代码': int,
        '自然日': int,
        '时间': int,  # keep concat date to datetime
        '成交编号': int,  # keep as ex_order_id
        '成交代码': str,  # keep as cancel, 深市 C / 0, 沪市 nan
        # '委托代码': str,
        'BS标志': str,  # keep as BS
        '成交价格': float,  # keep as price
        '成交数量': int,  # keep as volume
        '叫卖序号': int,  # keep as sell_id
        '叫买序号': int  # keep as buy_id
    }
    return pd.read_csv(
        csv_file, encoding='gbk',
        dtype=dtype_dict,
        usecols=list(dtype_dict.keys()),
        engine='pyarrow'
        # truncate_ragged_lines=True  # polars needed. Don't forget change pandas param dtype -> polars dtypes
    )  # .dropna(axis=1)


def get_trade_script() -> str:
    return (
        "select "
        "   concat(万得代码[-2:], 'SE.' , 万得代码[:6]) as jj_code, "
        "   strptime(concat(自然日, lpad(时间, 9, '0')), '%Y%m%d%H%M%S%g') as 自然日,"
        "   成交编号 as ex_order_id, 成交代码 as cancel, BS标志 as BS, 成交价格/10000 as price, 成交数量 as volume,"
        "   叫卖序号 as sell_id, 叫买序号 as buy_id "
        "from df"
    )  # duckdb %-H 不好使, 得自己补0


# def get_pd_process_trade_df(csv_file: Path):
#     cols_dict = {'万得代码': 'code', '自然日': 'date', '时间': 'time', 'BS标志': 'BS', '成交价格': 'price',
#                  '成交数量': 'volume', '叫卖序号': 'id_sell', '叫买序号': 'id_buy'}
#     keep_cols = ['jj_code', 'datetime', 'BS', 'price', 'volume', 'id_sell', 'id_buy']
#
#     df = get_trade_df(csv_file).dropna(axis=1).rename(columns=cols_dict)[list(cols_dict.values())]
#
#     df['datetime'] = pd.to_datetime(
#         df['date'].astype(str) + df['time'].astype(str).str.zfill(9), format='%Y%m%d%H%M%S%f')
#     df['jj_code'] = df['code'].apply(lambda x: f"{x[-2:]}SE.{x[:6]}" if len(x) == 9 else f"{x[-2:]}SE.{x[:5]}")
#     df['price'] = df['price'] / 1e4
#
#     return df[keep_cols]

def get_order_df(csv_file: Path) -> pd.DataFrame:
    dtype_dict = {
        '万得代码': str,  # keep convert to jj_code
        # '交易所代码': int,
        '自然日': str,
        '时间': str,  # keep concat date to datetime
        # '委托编号': int,
        '交易所委托号': int,  # keep as ex_order_id
        '委托类型': str,  # keep as order_type. 沪市, A 委托订单(增加), D 委托订单(删除); 深市, 0 ??1-市价, 2-限价, U-本方最优
        '委托代码': str,  # keep as BS
        '委托价格': int,  # keep as price
        '委托数量': int,  # keep as volume
    }
    return pd.read_csv(
        csv_file, encoding='gbk',
        dtype=dtype_dict,
        usecols=list(dtype_dict.keys()),
        engine='pyarrow'
    )  # .dropna(axis=1)


def get_order_script() -> str:
    return (
        "select "
        "   concat(万得代码[-2:], 'SE.' , 万得代码[:6]) as jj_code, "
        "   strptime(concat(自然日, lpad(时间, 9, '0')), '%Y%m%d%H%M%S%g') as 自然日,"
        "   交易所委托号 as ex_order_id, 委托类型 as order_type, 委托代码 as BS, "
        "   委托价格/10000 as price, 委托数量 as volume "
        "from df"
    ).dropna(axis=1)  # duckdb %-H 不好使, 得自己补0


ask_bid_dtype_dict = {
    # keep as ask_p1...
    '申卖价1': float, '申卖价2': float, '申卖价3': float, '申卖价4': float, '申卖价5': float,
    '申卖价6': float, '申卖价7': float, '申卖价8': float, '申卖价9': float, '申卖价10': float,
    # keep as ask_v1...
    '申卖量1': int, '申卖量2': int, '申卖量3': int, '申卖量4': int, '申卖量5': int,
    '申卖量6': int, '申卖量7': int, '申卖量8': int, '申卖量9': int, '申卖量10': int,
    # keep as bid_p1...
    '申买价1': float, '申买价2': float, '申买价3': float, '申买价4': float, '申买价5': float,
    '申买价6': float, '申买价7': float, '申买价8': float, '申买价9': float, '申买价10': float,
    # keep as bid_v1...
    '申买量1': int, '申买量2': int, '申买量3': int, '申买量4': int, '申买量5': int,
    '申买量6': int, '申买量7': int, '申买量8': int, '申买量9': int, '申买量10': int,
}


def get_orderbook_df(csv_file: Path) -> pd.DataFrame:
    dtype_dict = {
        '万得代码': str,  # keep convert to jj_code
        # '交易所代码': int,
        '自然日': str,
        '时间': str,  # keep concat date to datetime
        '成交价': float,  # keep as price
        '成交量': int,  # keep as volume
        '成交额': int,  # keep as amount
        '成交笔数': int,  # keep as trade_nums.
        # 'IOPV': str,  # ETF实时申赎数据??
        # '成交标志': str,  # nan
        # 'BS标志': str,  # nan
        '当日累计成交量': int,  # keep as cum_volume
        '当日成交额': int,  # keep as cum_amount
        '最高价': float,  # keep as high
        '最低价': float,  # keep as low
        '开盘价': float,  # keep as open
        '前收盘': float,  # keep as pre_close
        **ask_bid_dtype_dict,
        # keep as ask_volume, bid_volume
        '叫卖总量': int, '叫买总量': int,
        # keep as ask_avg_price, bid_avg_price
        '加权平均叫卖价': float, '加权平均叫买价': float,
    }

    return pd.read_csv(
        csv_file, encoding='gbk',
        dtype=dtype_dict, usecols=list(dtype_dict.keys()),
        engine='pyarrow'
    )  # .dropna(axis=1)


def get_orderbook_script() -> str:
    k_list = []
    for i, k in enumerate(ask_bid_dtype_dict.keys()):
        i = 10 if (i + 1) % 10 == 0 else (i + 1) % 10
        if '价' in k:
            p = f'{k}/10000'
            if '卖' in k:
                k_list.append(f'{p} as ask_p{i}')
            elif '买' in k:
                k_list.append(f'{p} as bid_p{i}')
        elif '量' in k:
            v = f'{k}'
            if '卖' in k:
                k_list.append(f'{v} as ask_v{i}')
            elif '买' in k:
                k_list.append(f'{v} as bid_v{i}')

    return (
        "select "
        "   concat(万得代码[-2:], 'SE.' , 万得代码[:6]) as jj_code, "
        "   strptime(concat(自然日, lpad(时间, 9, '0')), '%Y%m%d%H%M%S%g') as 自然日, "
        "   成交价/10000 as price, 成交量 as volume, 成交额 as amount, 成交笔数 as trade_nums, "
        "   当日累计成交量 as cum_volume, 当日成交额 as cum_amount, "
        "   最高价/10000 as high, 最低价/10000 as low, 开盘价/10000 as open, 前收盘/10000 as pre_close, "
        f"  {', '.join(k_list)}, 叫卖总量 as ask_volume, 叫买总量 as bid_volume, "
        "   加权平均叫卖价/10000 as ask_avg_price, 加权平均叫买价/10000 as bid_avg_price  "
        "from df"
    )


def update_res_list(file_path_list: List[Path], func: Callable[[Path], pd.DataFrame], res_list: ListProxy):
    for file in file_path_list:
        res_list.append(func(file))


class TaskConfig:
    def __init__(self, dir_path: Path, file_name: str, func: Callable[[Path], pd.DataFrame]):
        self.dir_path = dir_path
        self.file_name = file_name
        self.glob_file = fr'*\{file_name}.csv'
        self.func = func

    @property
    def files_list(self):
        return list(self.dir_path.glob(self.glob_file))


class Task:
    def __init__(self, dir_path: Path):
        self.trade = TaskConfig(dir_path=dir_path, file_name='逐笔成交', func=get_trade_df)
        self.order = TaskConfig(dir_path=dir_path, file_name='逐笔委托', func=get_order_df)
        self.orderbook = TaskConfig(dir_path=dir_path, file_name='行情', func=get_orderbook_df)


def get_level2_daily_df_with_multiprocess(task_cfg: TaskConfig) -> pd.DataFrame:
    pool = Pool(os.cpu_count() - 1)

    # set progress bar
    files_list = task_cfg.files_list
    step = int(len(files_list) / (4 * pool._processes))  # tune coe 4 get best speed
    pbar = tqdm(total=int(len(files_list) / step))
    pbar.set_description(
        f'Function get_level2_df_with_multiprocess {task_cfg.file_name} in {task_cfg.dir_path}, total {len(files_list)}, step {step}')

    # set multiprocess
    _manager = Manager()
    res_list = _manager.list()
    for i in range(0, len(files_list), step):
        pool.apply_async(
            update_res_list,
            args=(files_list[i:i + step], task_cfg.func, res_list),
            callback=lambda *args: pbar.update()
        )

    pool.close()
    pool.join()
    pbar.close()

    logger.info(f'Start concat dataframe ... ')
    start_time = time.perf_counter()

    # from itertools import chain
    #
    # def fast_flatten(input_list):
    #     return list(chain.from_iterable(input_list))
    #
    # COLUMN_NAMES = res_list[0].columns
    # df_dict = dict.fromkeys(COLUMN_NAMES, [])
    # for col in COLUMN_NAMES:
    #     extracted = (frame[col] for frame in res_list)
    #
    #     # Flatten and save to df_dict
    #     df_dict[col] = fast_flatten(extracted)
    # total_df = pd.DataFrame.from_dict(df_dict)[COLUMN_NAMES]

    # total_df = pl.concat(res_list)  # 57.6s
    total_df = pd.concat(res_list)  # with dropna col, time cost 57.3s -> 37.3s
    logger.success(f'Concat df cost: {time.perf_counter() - start_time}s')
    _manager.shutdown()

    gc.collect()

    return total_df


if __name__ == '__main__':
    import duckdb

    con = duckdb.connect()

    dir_path = Path(rf'E:\DuckDB\stock\trades\origin\20230508')
    # df = get_trade_df(dir_path / '000001.SZ' / '逐笔成交.csv')

    df1 = get_level2_daily_df_with_multiprocess(
        Task(dir_path).trade
    )

    df2 = get_level2_daily_df_with_multiprocess(
        Task(dir_path).order
    )

    df3 = get_level2_daily_df_with_multiprocess(
        Task(dir_path).orderbook
    )

    compress = 'ZSTD'
    start_time = time.perf_counter()
    con.sql('select * from df2').write_parquet(f'trade_{date_str}.parquet', compression=compress)
    print(f'cost: {time.perf_counter() - start_time}s')
