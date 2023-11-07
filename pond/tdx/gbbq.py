# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/27 1:34
# @Author   : Fangyang
# @Software : PyCharm


import time
from ctypes import c_uint32
from pathlib import Path
from typing import Union

import pandas as pd
from loguru import logger
from tqdm import tqdm

from gulf.tdx.gbbq_hexdump_keys import hexdump_keys
from gulf.tdx.path import gbbq_path


# take ref this article :http://blog.csdn.net/fangle6688/article/details/50956609
# and this http://blog.sina.com.cn/s/blog_6b2f87db0102uxo3.html
class GbbqReader:
    @staticmethod
    def get_df(filename: Union[Path, str]) -> pd.DataFrame:
        import struct
        import pandas as pd
        import sys

        if sys.version_info.major == 2:
            bin_keys = bytearray.fromhex(hexdump_keys)
        else:
            bin_keys = bytes.fromhex(hexdump_keys)
        result = []
        with open(filename, "rb") as f:
            content = f.read()
            pos = 0
            (count,) = struct.unpack("<I", content[pos: pos + 4])
            pos += 4
            encrypt_data = content
            # data_len = len(encrypt_data)
            data_offset = pos

            for _ in tqdm(range(count)):
                clear_data = bytearray()
                for i in range(3):
                    (eax,) = struct.unpack("<I", bin_keys[0x44: 0x44 + 4])
                    (ebx,) = struct.unpack("<I", encrypt_data[data_offset: data_offset + 4])
                    num = c_uint32(eax ^ ebx).value
                    (numold,) = struct.unpack("<I", encrypt_data[data_offset + 0x4: data_offset + 0x4 + 4])
                    for j in reversed(range(4, 0x40 + 4, 4)):
                        ebx = (num & 0xff0000) >> 16
                        (eax,) = struct.unpack("<I", bin_keys[ebx * 4 + 0x448: ebx * 4 + 0x448 + 4])
                        ebx = num >> 24
                        (eax_add,) = struct.unpack("<I", bin_keys[ebx * 4 + 0x48: ebx * 4 + 0x48 + 4])
                        eax += eax_add
                        eax = c_uint32(eax).value
                        ebx = (num & 0xff00) >> 8
                        (eax_xor,) = struct.unpack("<I", bin_keys[ebx * 4 + 0x848: ebx * 4 + 0x848 + 4])
                        eax ^= eax_xor
                        eax = c_uint32(eax).value
                        ebx = num & 0xff
                        (eax_add,) = struct.unpack("<I", bin_keys[ebx * 4 + 0xC48: ebx * 4 + 0xC48 + 4])
                        eax += eax_add
                        eax = c_uint32(eax).value
                        (eax_xor,) = struct.unpack("<I", bin_keys[j: j + 4])
                        eax ^= eax_xor
                        eax = c_uint32(eax).value
                        ebx = num
                        num = numold ^ eax
                        num = c_uint32(num).value
                        numold = ebx

                    (numold_op,) = struct.unpack("<I", bin_keys[0: 4])
                    numold ^= numold_op
                    numold = c_uint32(numold).value
                    clear_data.extend(struct.pack("<II", numold, num))
                    data_offset += 8

                clear_data.extend(encrypt_data[data_offset: data_offset + 5])

                (v1, v2, v3, v4, v5, v6, v7, v8) = (struct.unpack("<B7sIBffff", clear_data))
                line = (v1,
                        v2.rstrip(b"\x00").decode("utf-8"),
                        v3,
                        v4,
                        v5,
                        v6,
                        v7,
                        v8)
                result.append(line)
                data_offset += 5

        df = pd.DataFrame(
            data=result,
            columns=['market', 'code', 'datetime', 'category', 'hongli_panqianliutong', 'peigujia_qianzongguben',
                     'songgu_qianzongguben',
                     'peigu_houzongguben'])
        return df


def gen_gbbq_df():
    start_time = time.perf_counter()
    category = {
        1: '除权除息', 2: '送配股上市', 3: '非流通股上市', 4: '未知股本变动', 5: '股本变化',
        6: '增发新股', 7: '股份回购', 8: '增发新股上市', 9: '转配股上市', 10: '可转债上市',
        11: '扩缩股', 12: '非流通股缩股', 13: '送认购权证', 14: '送认沽权证'
    }

    logger.info(f'Start to decode gbbq(股本变迁) file...')
    gbbq_df = GbbqReader().get_df(gbbq_path / 'gbbq')
    gbbq_df.drop(columns=['market'], inplace=True)
    gbbq_df.columns = ['code', '权息日', '类别', '分红-前流通盘', '配股价-前总股本', '送转股-后流通盘', '配股-后总股本']
    gbbq_df['code'] = gbbq_df['code'].astype('object')

    # start_time = time.perf_counter()
    # optimize for loop from 31s -> 0.02s
    gbbq_df['类别'] = gbbq_df['类别'].replace(category)
    # print(f'apply cost:{time.perf_counter()-start_time:.4f}s')

    csv_file = gbbq_path / 'gbbq.csv'
    gbbq_df.to_csv(csv_file, index=False)
    logger.success(
        f'Decode gbbq(股本变迁) file, save in {csv_file}, time cost: {(time.perf_counter() - start_time):.2f}s'
    )


if __name__ == '__main__':
    # result = GbbqReader().get_df(gbbq_path / 'gbbq')
    # print(result)
    gen_gbbq_df()
