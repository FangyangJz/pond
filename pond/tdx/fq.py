# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/26 20:54
# @Author   : Fangyang
# @Software : PyCharm

import datetime
import os
import time
from multiprocessing import Pool, Manager
from multiprocessing.managers import DictProxy
from multiprocessing.pool import Pool as PoolType
from typing import Dict, Union

import numpy as np
import pandas as pd
from loguru import logger

from tqdm import tqdm

from gulf.dolphindb.tables import StockBasicTable
from mootdx.reader import Reader
from gulf.dolphindb.stock import StockDB
from gulf.tdx.finance_cw import get_cw_dict_acc
from gulf.tdx.path import gbbq_path, tdx_path


def update_res_dict(
        stock_df: pd.DataFrame, gbbq_df: pd.DataFrame,
        cw_dict: Dict[str, pd.DataFrame], res_dict: DictProxy, offset: int
):
    reader = Reader.factory(market='std', tdxdir=tdx_path)

    for idx, row in stock_df.iterrows():
        stock_code = row['代码']

        stock_daily_df = reader.daily(symbol=stock_code)
        stock_daily_df = stock_daily_df if offset >= 0 else stock_daily_df.iloc[offset:]
        if stock_daily_df.empty:
            continue

        stock_daily_df['jj_code'] = row['jj_code']
        stock_daily_df['name'] = row['名称']
        stock_daily_df['industry'] = row['所处行业']

        # process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
        # df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + filename, index_col=None, encoding='gbk',
        #                      dtype={'code': str})

        df_qfq = make_fq(code=stock_code, code_df=stock_daily_df, gbbq_df=gbbq_df, cw_dict=cw_dict)
        res_dict[stock_code] = df_qfq


def qfq_acc(gbbq_df: pd.DataFrame, cw_dict: Dict[str, pd.DataFrame], pool: PoolType = None) -> Dict[str, pd.DataFrame]:
    build_in_pool = False
    if not isinstance(pool, PoolType):
        logger.info(f'Not pass multiprocess pool in parameter, build in function.')
        build_in_pool = True
        pool = Pool(os.cpu_count() - 1)
    else:
        logger.info('Use external multiprocess pool.')

    logger.info(
        f'Start to update_res_dict with multiprocess. Process nums:{pool._processes}, state:{pool._state}')

    # set database and get stock basic data
    offset = 1
    db = StockDB()
    # Tips: For debug use dataframe slice
    stock_basic_df = db.get_dimension_table_df(StockBasicTable, from_db=True)  # .iloc[:1000]

    # set progress bar
    step = int(len(stock_basic_df) / (4 * pool._processes))  # tune coe 4 get best speed
    pbar = tqdm(total=int(len(stock_basic_df) / step))
    pbar.set_description(f'Function qfq_acc(), total {len(stock_basic_df)}, step {step}')

    # set multiprocess
    _manager = Manager()
    res_dict = _manager.dict()
    for i in range(0, len(stock_basic_df), step):
        pool.apply_async(
            update_res_dict,
            args=(stock_basic_df.iloc[i:i + step], gbbq_df, cw_dict, res_dict, offset),
            callback=lambda *args: pbar.update()
        )

    if build_in_pool:
        pool.close()
        pool.join()

    res_dict = dict(res_dict)
    _manager.shutdown()

    return res_dict


def qfq(gbbq_df: pd.DataFrame, cw_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    # set database and get stock basic data
    offset = 1
    db = StockDB()
    # Tips: For debug use dataframe slice
    stock_basic_df = db.get_dimension_table_df(StockBasicTable, from_db=True)  # .iloc[:1000]

    logger.info('Function qfq with 1 process...')
    res_dict = dict()
    update_res_dict(stock_basic_df, gbbq_df, cw_dict, res_dict, offset)

    return res_dict


def make_fq(
        code: str, code_df: pd.DataFrame, gbbq_df: pd.DataFrame,
        cw_dict: Union[Dict[str, pd.DataFrame], None] = None,
        start_date='', end_date='', fqtype: str = 'qfq'
) -> pd.DataFrame:
    """
    股票周期数据复权处理函数
    :param code:str格式，具体股票代码
    :param code_df:DF格式，未除权的具体股票日线数据。DF自动生成的数字索引，列定义：date,open,high,low,close,vol,amount
    :param gbbq_df:DF格式，通达信导出的全股票全日期股本变迁数据。DF读取gbbq文件必须加入dtype={'code': str}参数，否则股票代码开头0会忽略
    :param cw_dict:DF格式，读入内存的全部财务文件
    :param start_date:可选，要截取的起始日期。默认为空。格式"2020-10-10"
    :param end_date:可选，要截取的截止日期。默认为空。格式"2020-10-10"
    :param fqtype:可选，复权类型。默认前复权。
    :return:复权后的DF格式股票日线数据
    """

    '''以下是从https://github.com/rainx/pytdx/issues/78#issuecomment-335668322 提取学习的前复权代码
    import datetime

    import numpy as np
    import pandas as pd
    from pytdx.hq import TdxHq_API
    # from pypinyin import lazy_pinyin
    import tushare as ts

    '除权除息'
    api = TdxHq_API()

    with api.connect('180.153.39.51', 7709):
        # 从服务器获取该股的股本变迁数据
        category = {
            '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
            '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
            '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}
        data = api.to_df(api.get_xdxr_info(0, '000001'))
        data = data \
            .assign(date=pd.to_datetime(data[['year', 'month', 'day']])) \
            .drop(['year', 'month', 'day'], axis=1) \
            .assign(category_meaning=data['category'].apply(lambda x: category[str(x)])) \
            .assign(code=str('000001')) \
            .rename(index=str, columns={'panhouliutong': 'liquidity_after',
                                        'panqianliutong': 'liquidity_before', 'houzongguben': 'shares_after',
                                        'qianzongguben': 'shares_before'}) \
            .set_index('date', drop=False, inplace=False)
        xdxr_data = data.assign(date=data['date'].apply(lambda x: str(x)[0:10]))  # 该股的股本变迁DF处理完成
        df_gbbq = xdxr_data[xdxr_data['category'] == 1]  # 提取只有除权除息的行保存到DF df_gbbq
        # print(df_gbbq)

        # 从服务器读取该股的全部历史不复权K线数据，保存到data表，  只包括 日期、开高低收、成交量、成交金额数据
        data = pd.concat([api.to_df(api.get_security_bars(9, 0, '000001', (9 - i) * 800, 800)) for i in range(10)], axis=0)

        # 从data表加工数据，保存到bfq_data表
        df_code = data \
            .assign(date=pd.to_datetime(data['datetime'].apply(lambda x: x[0:10]))) \
            .assign(code=str('000001')) \
            .set_index('date', drop=False, inplace=False) \
            .drop(['year', 'month', 'day', 'hour',
                   'minute', 'datetime'], axis=1)
        df_code['if_trade'] = True
        # 不复权K线数据处理完成，保存到bfq_data表

        # 提取info表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是除权除息日的行
        data = pd.concat([df_code, df_gbbq[['category']][df_code.index[0]:]], axis=1)
        # print(data)

        data['date'] = data.index
        data['if_trade'].fillna(value=False, inplace=True)  # if_trade列，无效的值填充为False
        data = data.fillna(method='ffill')  # 向下填充无效值

        # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
        # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
        data = pd.concat([data, df_gbbq[['fenhong', 'peigu', 'peigujia',
                                      'songzhuangu']][df_code.index[0]:]], axis=1)
        data = data.fillna(0)  # 无效值填空0

        data['preclose'] = (data['close'].shift(1) * 10 - data['fenhong'] + data['peigu']
                            * data['peigujia']) / (10 + data['peigu'] + data['songzhuangu'])
        data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1)[::-1].cumprod()  # 计算每日复权因子
        data['open'] = data['open'] * data['adj']
        data['high'] = data['high'] * data['adj']
        data['low'] = data['low'] * data['adj']
        data['close'] = data['close'] * data['adj']
        data['preclose'] = data['preclose'] * data['adj']

        data = data[data['if_trade']]
        result = data \
            .drop(['fenhong', 'peigu', 'peigujia', 'songzhuangu', 'if_trade', 'category'], axis=1)[data['open'] != 0] \
            .assign(date=data['date'].apply(lambda x: str(x)[0:10]))
        print(result)
    '''

    # 先进行判断。如果有adj列，且没有NaN值，表示此股票数据已处理完成，无需处理。直接返回。
    # 如果没有‘adj'列，表示没进行过复权处理，当作新股处理
    if 'adj' in code_df.columns:
        if True in code_df['adj'].isna().to_list():
            first_index = np.where(code_df.isna())[0][0]  # 有NaN值，设为第一个NaN值所在的行
        else:
            return ""
    else:
        first_index = 0
        flag_newstock = True

    flag_attach = False  # True=追加数据模式  False=数据全部重新计算
    # 设置新股标志。True=新股，False=旧股。新股跳过追加数据部分的代码。如果没定义，默认为False
    if 'flag_newstock' not in dir():
        flag_newstock = False

    # 提取该股除权除息行保存到DF df_cqcx，提取其他信息行到df_gbbq
    df_cqcx = gbbq_df.loc[(gbbq_df['code'] == code) & (gbbq_df['类别'] == '除权除息')]
    gbbq_df = gbbq_df.loc[(gbbq_df['code'] == code) & (gbbq_df['类别'].isin(['股本变化', '送配股上市', '转配股上市']))]

    # 清洗df_gbbq，可能出现同一日期有 配股上市、股本变化两行数据。不清洗后面合并会索引冲突。
    # 下面的代码可以保证删除多个不连续的重复行，用DF dropdup方法不能确保删除的值是大是小
    # 如果Ture在列表里。表示有重复行存在
    if True in gbbq_df.duplicated(subset=['权息日'], keep=False).to_list():
        #  提取重复行的索引
        del_index = []  # 要删除的后流通股的值
        tmp_dict = gbbq_df.duplicated(subset=['权息日'], keep=False).to_dict()
        for k, v in tmp_dict.items():
            if v:
                del_index.append(gbbq_df.at[k, '送转股-后流通盘'])
                # 如果dup_index有1个以上的值，且K+1的元素是False，或K+1不存在也返回False，表示下一个元素 不是 重复行
                if len(del_index) > 1 and (tmp_dict.get(k + 1, False) == False):
                    del_index.remove(max(del_index))  # 删除最大值
                    # 选择剩余的值，取反，则相当于保留了最大值，删除了其余的值
                    gbbq_df = gbbq_df[~gbbq_df['送转股-后流通盘'].isin(del_index)]

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04 为了按日期一一对应拼接
    df_cqcx = df_cqcx.assign(date=pd.to_datetime(df_cqcx['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    df_cqcx.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    df_cqcx['category'] = 1.0  # 添加category列
    gbbq_df = gbbq_df.assign(date=pd.to_datetime(gbbq_df['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    gbbq_df.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    if len(df_cqcx) > 0:  # =0表示股本变迁中没有该股的除权除息信息。gbbq_lastest_date设置为今天，当作新股处理
        cqcx_lastest_date = df_cqcx.index[-1]  # 提取最新的除权除息日
    else:
        cqcx_lastest_date = str(datetime.date.today())
        flag_newstock = True

    # 判断df_code是否已有历史数据，是追加数据还是重新生成。
    # 如果gbbq_lastest_date not in df_code.loc[first_index:, 'date'].to_list()，表示未更新数据中不包括除权除息日
    # 由于前复权的特性，除权后历史数据都要变。因此未更新数据中不包括除权除息日，只需要计算未更新数据。否则日线数据需要全部重新计算
    # 如果'adj'在df_code的列名单里，表示df_code是已复权过的，只需追加新数据，否则日线数据还是需要全部重新计算
    if cqcx_lastest_date not in code_df.index and not flag_newstock:
        if 'adj' in code_df.columns.to_list():
            flag_attach = True  # 确定为追加模式
            df_code_original = code_df  # 原始code备份为df_code_original，最后合并
            code_df = code_df.iloc[first_index:]  # 切片df_code，只保留需要处理的行
            code_df.reset_index(drop=True, inplace=True)
            df_code_original.dropna(how='any', inplace=True)  # 丢掉缺失数据的行，之后直接append新数据就行。比merge简单。
            df_code_original['date'] = pd.to_datetime(df_code_original['date'], format='%Y-%m-%d')  # 转为时间格式
            df_code_original.set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    # 单独提取流通股处理。因为流通股是设置流通股变更时间节点，最后才填充nan值。和其他列的处理会冲突。
    # 如果有流通股列，单独复制出来；如果没有流通股列，添加流通股列，赋值为NaN。
    # 如果是追加数据模式，则肯定已存在流通股列且数据已处理。因此不需单独提取流通股列。只在前复权前处理缺失的流通股数据即可
    # 虽然财报中可能没有流通股的数据，但股本变迁文件中最少也有股票第一天上市时的流通股数据。
    # 且后面还会因为送配股上市、股本变化，导致在非财报日之前，流通股就发生变动
    if not flag_attach:
        if '流通股' in code_df.columns.to_list():
            df_ltg = pd.DataFrame(index=code_df.index)
            df_ltg['date'] = code_df['date']
            df_ltg['流通股'] = code_df['流通股']
            del code_df['流通股']
        else:
            df_ltg = pd.DataFrame(code_df.index)
            # df_ltg['date'] = df_code['date']
            df_ltg['流通股'] = np.nan
    else:
        # 附加模式，此处df_code是已经切片过的，只包括需要更新的数据行。其中也包含流通股列，值全为NaN。
        # 类似单独提出处理流通股列，和新股模式的区别是只处理需要更新的数据行。
        df_ltg = pd.DataFrame(index=code_df.index)
        del code_df['流通股']
        # 第一个值赋值为df_code_original流通股列第一个NaN值的前一个有效值
        ltg_lastest_value = df_code_original.at[df_code_original.index[-1], '流通股']
        df_ltg['date'] = code_df['date']
        df_ltg['流通股'] = np.nan
        df_ltg.at[0, '流通股'] = ltg_lastest_value
    gbbq_df = gbbq_df.rename(columns={'送转股-后流通盘': '流通股'})  # 列改名，为了update可以匹配
    # 用df_gbbq update data，由于只有流通股列重复，因此只会更新流通股列对应索引的NaN值
    # df_ltg['date'] = pd.to_datetime(df_ltg['date'], format='%Y-%m-%d')  # 转为时间格式
    df_ltg.set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    df_ltg.update(gbbq_df, overwrite=False)  # 使用update方法更新df_ltg
    if not flag_attach:  # 附加模式则单位已经调整过，无需再调整
        # 股本变迁里的流通股单位是万股。转换与财报的单位：股 统一
        df_ltg['流通股'] = df_ltg['流通股'] * 10000

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04  为了按日期一一对应拼接
    # with pd.option_context('mode.chained_assignment', None):  # 临时屏蔽语句警告
    #     df_code['date'] = pd.to_datetime(df_code['date'], format='%Y-%m-%d')
    # df_code.set_index('date', drop=True, inplace=True)
    code_df.insert(code_df.shape[1], 'if_trade', True)  # 插入if_trade列，赋值True

    # 提取df_cqcx和df_gbbq表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是股本变迁的行
    data = pd.concat([code_df, df_cqcx[['category']][code_df.index[0]:]], axis=1)
    # print(data)

    data['if_trade'].fillna(value=False, inplace=True)  # if_trade列，无效的值填充为False
    data.fillna(method='ffill', inplace=True)  # 向下填充无效值

    # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
    # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
    data = pd.concat([data, df_cqcx[['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                                     '送转股-后流通盘']][code_df.index[0]:]], axis=1)
    data = data.fillna(0)  # 无效值填空0
    data['preclose'] = (data['close'].shift(1) * 10 - data['分红-前流通盘'] + data['配股-后总股本']
                        * data['配股价-前总股本']) / (10 + data['配股-后总股本'] + data['送转股-后流通盘'])
    # 计算每日复权因子 前复权最近一次股本变迁的复权因子为1
    data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1)[::-1].cumprod()
    data['open'] = data['open'] * data['adj']
    data['high'] = data['high'] * data['adj']
    data['low'] = data['low'] * data['adj']
    data['close'] = data['close'] * data['adj']
    # data['preclose'] = data['preclose'] * data['adj']  # 这行没用了
    data = data[data['if_trade']]  # 重建整个表，只保存if_trade列=true的行

    # 抛弃过程处理行，且open值不等于0的行, 复权处理完成
    data = data.drop(['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                      '送转股-后流通盘', 'if_trade', 'category', 'preclose'], axis=1)[data['open'] != 0]

    # 计算换手率
    # 财报数据公开后，股本才变更。因此有效时间是“当前财报日至未来日期”。故将结束日期设置为2099年。每次财报更新后更新对应的日期时间段
    e_date = '20990101'
    for cw_date in cw_dict:  # 遍历财报字典  cw_date=财报日期  cw_dict[cw_date]=具体的财报内容
        # 如果复权数据表的首行日期>当前要读取的财务报表日期，则表示此财务报表发布时股票还未上市，跳过此次循环。有例外情况：003001
        # (cw_dict[cw_date][0] == code).any() 表示当前股票code在财务DF里有数据
        if df_ltg.index[0].strftime('%Y%m%d') <= cw_date <= df_ltg.index[-1].strftime('%Y%m%d') and len(
                cw_dict[cw_date]) > 0:
            if (cw_dict[cw_date][0] == code).any():
                # 获取目前股票所在行的索引值，具有唯一性，所以直接[0]
                code_df_index = cw_dict[cw_date][cw_dict[cw_date][0] == code].index.to_list()[0]
                # DF格式读取的财报，字段与财务说明文件的序号一一对应，如果是CSV读取的，字段需+1
                # print(f'{cw_date} 总股本:{cw_dict[cw_date].iat[code_df_index,238]}'
                #  f'流通股本:{cw_dict[cw_date].iat[code_df_index,239]}')
                # 如果流通股值是0，则进行下一次循环
                if int(cw_dict[cw_date].iat[code_df_index, 239]) != 0:
                    #  df_ltg[cw_date:e_date].index[0] 表示df_ltg中从cw_date到e_date的第一个索引的值。
                    #  也就是离cw_date日期最近的下一个有效行
                    df_ltg.at[df_ltg[cw_date:e_date].index[0], '流通股'] = float(
                        cw_dict[cw_date].iat[code_df_index, 239])

    # df_ltg拼接回原DF
    data = pd.concat([data, df_ltg], axis=1)

    data = data.fillna(method='ffill')  # 向下填充无效值
    data = data.fillna(method='bfill')  # 向上填充无效值  为了弥补开始几行的空值
    data = data.round({'open': 2, 'high': 2, 'low': 2, 'close': 2, })  # 指定列四舍五入
    if '流通股' in data.columns.to_list():
        data['流通市值'] = data['流通股'] * data['close']
        data['换手率'] = data['volume'] / data['流通股'] * 100
        data = data.round({'流通市值': 2, '换手率': 5, })  # 指定列四舍五入
    if flag_attach:  # 追加模式，则附加最新处理的数据
        data = df_code_original.append(data)

    if len(start_date) == 0 and len(end_date) == 0:
        pass
    elif len(start_date) != 0 and len(end_date) == 0:
        data = data[start_date:]
    elif len(start_date) == 0 and len(end_date) != 0:
        data = data[:end_date]
    elif len(start_date) != 0 and len(end_date) != 0:
        data = data[start_date:end_date]
    data.reset_index(drop=False, inplace=True)  # 重置索引行，数字索引，date列到第1列，保存为str '1991-01-01' 格式
    # 最后调整列顺序
    # data = data.reindex(
    # columns=['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adj', '流通股', '流通市值', '换手率'])
    return data


if __name__ == '__main__':
    df_gbbq = pd.read_csv(gbbq_path / 'gbbq.csv', dtype={'code': str})

    #########################
    build_in_pool_start_time = time.perf_counter()

    cw_dict = get_cw_dict_acc()

    qfq_acc_start_time = time.perf_counter()
    res_dict1 = qfq_acc(df_gbbq, cw_dict)
    qfq_acc_cost_time = time.perf_counter() - qfq_acc_start_time

    build_in_pool_cost_time = time.perf_counter() - build_in_pool_start_time

    ########################
    qfq_start_time = time.perf_counter()
    res_dict2 = qfq(df_gbbq, cw_dict)
    qfq_cost_time = time.perf_counter() - qfq_start_time

    logger.success(f'qfq_acc_cost_time:{qfq_acc_cost_time:.4f}s')
    logger.success(f'qfq_cost_time:{qfq_cost_time:.4f}s')
    logger.success(f'build_in_pool_cost_time:{build_in_pool_cost_time:.4f}s')

    print(1)

    #########################
    # 暂时不要使用从外传入进程池的方式, 因为共用进程池的时候 qfq_acc 使用的 cw_dict 不完整,
    # 而且还存在 manager 关闭的问题
    # build_ex_pool_start_time = time.perf_counter()
    # p = Pool(os.cpu_count() - 1)
    # cw_dict2 = get_cw_dict_acc(pool=p)
    # res_dict3 = qfq_acc(df_gbbq, cw_dict2, pool=p)
    # p.close()
    # p.join()
    # build_ex_pool_cost_time = time.perf_counter() - build_ex_pool_start_time

    # logger.success(f'build_ex_pool_cost_time:{build_ex_pool_cost_time:.4f}s')
    # print(1)
