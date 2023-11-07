# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/10/26 23:08
# @Author   : Fangyang
# @Software : PyCharm


from mootdx.reader import Reader

reader = Reader.factory(tdxdir=r'C:\new_tdx')
a = reader.block(symbol='block')
b = reader.block(symbol='block_zs')
c = reader.block(symbol='block_zs', group=True)
d = reader.block(symbol='incon')

print(1)

if __name__ == '__main__':
    pass
