# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/26 22:49
# @Author   : Fangyang
# @Software : PyCharm

import os
import time
import zipfile
from multiprocessing import Pool, Manager
from multiprocessing.pool import Pool as PoolType
from pathlib import Path
from typing import List, Union, Dict

import pandas as pd
from mootdx.affair import Affair
from gulf.tdx.path import cw_path
from loguru import logger
import struct


def get_local_cw_file_list(ext_name='.dat') -> List[Path]:
    """
    列出本地已有的专业财务文件。返回文件列表
    :param ext_name: str类型。文件扩展名。返回指定扩展名的文件列表
    :return: list类型。财务专业文件列表
    """
    return [f for f in cw_path.glob("*" + ext_name) if len(f.stem) == 12 and f.stem[:4] == "gpcw"]


def update_cw_data():
    local_files = [f.name for f in cw_path.glob('*.zip')]  # 本地财务文件列表
    remote_files = Affair.files()  # 远程财务文件列表

    if len(remote_files) == len(local_files):
        logger.info(f"Remote file length == local file length, no need to update.")

    for remote_file in remote_files:
        remote_filename = remote_file['filename']
        if remote_filename not in local_files:
            logger.info(f"{remote_filename} not in local, start to download.")
            # 下载单个
            Affair.fetch(downdir=str(cw_path), filename=remote_filename)

            with zipfile.ZipFile(cw_path / remote_filename, 'r') as zipobj:  # 打开zip对象，释放zip文件。会自动覆盖原文件。
                zipobj.extractall(cw_path)


def get_history_financial_df(filepath: Union[Path, str]) -> pd.DataFrame:
    """
    读取解析通达信目录的历史财务数据
    :param filepath: 字符串类型。传入文件路径
    :return: DataFrame格式。返回解析出的财务文件内容
    """
    with open(filepath, 'rb') as cw_file:
        header_pack_format = '<1hI1H3L'
        header_size = struct.calcsize(header_pack_format)
        stock_item_size = struct.calcsize("<6s1c1L")
        data_header = cw_file.read(header_size)
        stock_header = struct.unpack(header_pack_format, data_header)
        max_count = stock_header[2]
        report_date = stock_header[1]
        report_size = stock_header[4]
        report_fields_count = int(report_size / 4)
        report_pack_format = f'<{report_fields_count}f'

        results = []
        for stock_item in struct.iter_unpack("<6s1c1L", cw_file.read(max_count * struct.calcsize("<6s1c1L"))):
            code = stock_item[0].decode("utf-8")
            foa = stock_item[2]
            cw_file.seek(foa)
            info_data = cw_file.read(struct.calcsize(report_pack_format))
            data_size = len(info_data)
            cw_info = list(struct.unpack(report_pack_format, info_data))
            cw_info.insert(0, code)
            cw_info.insert(1, filepath.stem[4:])
            results.append(cw_info)

    return pd.DataFrame(results)


def update_cw_dict(part_local_cw_file_list: List[Path], cw_dict: Dict[str, pd.DataFrame]):
    for i in part_local_cw_file_list:
        cw_df = get_history_financial_df(i)
        if cw_df.empty:
            continue
        cw_dict[i.stem[4:]] = cw_df


def get_cw_dict() -> Dict[str, pd.DataFrame]:
    logger.info('Loading local financial data with 1 process...')
    cw_dict = {}
    update_cw_dict(get_local_cw_file_list(), cw_dict)
    return cw_dict


def get_cw_dict_acc(pool: PoolType = None) -> Dict[str, pd.DataFrame]:
    from tqdm import tqdm

    build_in_pool = False
    if not isinstance(pool, PoolType):
        logger.info(f'Not pass multiprocess pool in parameter, build in function.')
        build_in_pool = True
        pool = Pool(os.cpu_count() - 1)
    else:
        logger.info('Use external multiprocess pool.')

    logger.info(
        f'Start to load local financial data with multiprocess. Process nums:{pool._processes}, state:{pool._state}')

    local_cw_file_list = get_local_cw_file_list()
    step = int(len(local_cw_file_list) / (4 * pool._processes))  # tune coe 4 get best speed
    pbar = tqdm(total=int(len(local_cw_file_list) / step))
    pbar.set_description(f'Function get_cw_dict_acc() total {len(local_cw_file_list)}, step {step}')

    # set multiprocess
    _manager = Manager()
    cw_dict = _manager.dict()
    for i in range(0, len(local_cw_file_list), step):
        list_r = len(local_cw_file_list) if i + step > len(local_cw_file_list) else i + step
        pool.apply_async(
            update_cw_dict,
            args=(local_cw_file_list[i:list_r], cw_dict),
            callback=lambda *args: pbar.update()
        )

    if build_in_pool:
        pool.close()
        pool.join()

    # use self method copy value, slow 2s
    # cw_dict = cw_dict._getvalue()
    cw_dict = dict(cw_dict)
    _manager.shutdown()

    return cw_dict


if __name__ == '__main__':
    update_cw_data()
    file_list = get_local_cw_file_list()

    start_time = time.perf_counter()
    cw_test_dict1 = get_cw_dict_acc()
    logger.success(f'{os.cpu_count() - 1} process time cost: {time.perf_counter() - start_time:.4f}s')

    start_time = time.perf_counter()
    cw_test_dict2 = get_cw_dict()
    logger.success(f'1 process time cost: {time.perf_counter() - start_time:.4f}s')
    print(1)
