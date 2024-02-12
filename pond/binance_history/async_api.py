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
from pond.utils.crawler import get_mock_headers
from httpx._types import ProxiesTypes


async def download_file(url: str, path: Path, proxies: ProxiesTypes = {}):
    async with httpx.AsyncClient(proxies=proxies) as client:
        response = await client.get(url, timeout=None, headers=get_mock_headers())

        if response.status_code == 200:
            logger.success(f"[{response.status_code}] {url}")

            file_path = path / urlparse(url).path[1:]
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(response.content)
                logger.success(f"Save to {file_path}")
        else:
            logger.error(f"[{response.status_code}] {url}")


async def download_files(url_list: List[str], path: Path, proxies: ProxiesTypes = {}):
    tasks = []
    for url in url_list:
        task = asyncio.create_task(download_file(url, path, proxies))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def download_zip_files(
    url_list: List[str], path: Path, proxies: ProxiesTypes = {}
):
    await download_files(url_list, path, proxies)


def start_async_download_files(
    url_list: List[str], path: Path, proxies: ProxiesTypes = {}
):
    asyncio.run(download_zip_files(url_list, path, proxies))


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
