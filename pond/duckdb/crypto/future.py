# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2023/12/23 下午 09:18
# @Author   : Fangyang
# @Software : PyCharm


from binance.cm_futures import CMFutures

# https://github.com/binance/binance-futures-connector-python

proxies = {'https': '127.0.0.1:7890'}

client = CMFutures(
    proxies=proxies
)

# r = client.klines("BTCUSD_PERP", "1d")
info = client.exchange_info()
cm_list = []
cm_symbol_list = []
for i in info['symbols']:
    if i['contractType'] == 'PERPETUAL':
        cm_list.append(i)
        cm_symbol_list.append(i['symbol'])
r = client.continuous_klines("BTCUSD", "PERPETUAL", "1m")
print(1)


if __name__ == "__main__":
    pass
