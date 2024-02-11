# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Datetime : 2024/2/10 下午 04:57
# @Author   : Fangyang
# @Software : PyCharm

import httpx
import asyncio

from typing import List
from pathlib import Path
from loguru import logger
from urllib.parse import urlparse


# 定义一个异步函数，用于下载一个 url 的文件，并保存到指定的目录
async def download_file(url: str, path: Path):
    headers = {
        "User-Agent": "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
    }

    # 使用 httpx.AsyncClient 来发送请求
    async with httpx.AsyncClient() as client:
        # 获取响应
        response = await client.get(url, timeout=None, headers=headers)
        # 检查状态码是否为 200
        if response.status_code == 200:
            # 打印成功的信息
            logger.success(f"[{response.status_code}] {url}")

            file_path = path / urlparse(url).path[1:]
            file_path.parent.mkdir(parents=True, exist_ok=True)
            # 以二进制模式打开文件
            with open(file_path, "wb") as f:
                # 写入响应的内容
                f.write(response.content)
                logger.success(f"Save to {file_path}")
        else:
            # 打印失败的信息
            logger.error(f"[{response.status_code}] {url}")


async def download_files(url_list: List[str], path: Path):
    tasks = []
    for url in url_list:
        # 创建一个下载任务，并添加到列表中
        task = asyncio.create_task(download_file(url, path))
        tasks.append(task)
    # 等待所有任务完成
    await asyncio.gather(*tasks)


# 定义一个主函数，用于调用下载函数
async def download_zip_files(url_list: List[str], path: Path):
    await download_files(url_list, path)


def start_async_download_files(url_list: List[str], path: Path):
    asyncio.run(download_zip_files(url_list, path))


if __name__ == "__main__":
    from zipfile import ZipFile
    import polars as pl

    # df = pl.read_csv(ZipFile("results/DEFIUSDT-1m-2023-05.zip").read("DEFIUSDT-1m-2023-05.csv")).to_pandas()
    url_list = [
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-05.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-06.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-07.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-1998-07.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-08.zip",
    ]

    start_async_download_files(url_list, Path("./temp"))
