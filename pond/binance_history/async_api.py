# !/usr/bin/env python3
# @Datetime : 2024/2/10 下午 04:57
# @Author   : Fangyang
# @Software : PyCharm
from typing import Callable, Any

import httpx
import asyncio
from pathlib import Path

from loguru import logger
from urllib.parse import urlparse
from httpx._types import ProxyTypes
from tenacity import retry, stop_after_attempt, wait_fixed

from pond.utils.crawler import get_mock_headers


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
async def download_file(url: str, path: Path, proxies: ProxyTypes = {}):
    async with httpx.AsyncClient(proxies=proxies) as client:
        response = await client.get(url, timeout=30, headers=get_mock_headers())

        if response.status_code == 200:
            logger.success(f"[{response.status_code}] {url}")

            file_path = path / urlparse(url).path[1:]
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(response.content)
                logger.success(f"Save to {file_path}")
        else:
            error_msg = f"[{response.status_code}] {url}"
            logger.error(error_msg)
            raise Exception(error_msg)


async def async_tasks(
    url_list: list[str],
    async_func: Callable,
    func_param_list: list[dict[str, Any]],
):
    tasks = []
    for url in url_list:
        for func_params in func_param_list:
            task = asyncio.create_task(async_func(url=url, **func_params))
            tasks.append(task)
    await asyncio.gather(*tasks)


def start_async_download_files(
    url_list: list[str], path: Path, proxies: ProxyTypes = {}
):
    func_param_list = [{"path": path, "proxies": proxies}]
    logger.info("Running asyncio tasks ......")
    asyncio.run(async_tasks(url_list, download_file, func_param_list))


if __name__ == "__main__":
    # df = pl.read_csv(ZipFile("results/DEFIUSDT-1m-2023-05.zip").read("DEFIUSDT-1m-2023-05.csv")).to_pandas()
    url_list = [
        # https://data.binance.vision/data/spot/monthly/klines/BCCBTC/1d/BCCBTC-1d-2018-10.zip
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-05.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-06.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-07.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-1998-07.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-08.zip",
    ]

    start_async_download_files(url_list, Path("./temp"))
