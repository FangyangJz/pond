# !/usr/bin/env python3
# @Datetime : 2024/2/10 下午 04:57
# @Author   : Fangyang
# @Software : PyCharm
"""
文件下载模块

推荐使用 download_manager.py 中的 DownloadManager 获得更好的性能。
此模块保留向后兼容性。
"""
from typing import Callable, Any
from pathlib import Path

from loguru import logger

from pond.binance_history.download_manager import get_download_manager


def start_async_download_files(
    url_list: list[str],
    path: Path,
    proxies: dict[str, str] = {},
    max_concurrent: int = 10,  # noqa: ARG001 - 保留向后兼容
):
    """
    在多线程安全的方式下启动下载。

    Args:
        url_list: 要下载的 URL 列表
        path: 保存路径
        proxies: 代理设置（首次调用时生效，后续忽略）
        max_concurrent: 已废弃，使用全局 DownloadManager 配置

    Note:
        此函数使用全局 DownloadManager，共享连接池和线程池。
        首次调用会初始化 DownloadManager，后续调用复用同一实例。
    """
    if not url_list:
        return

    logger.info(f"Downloading {len(url_list)} files...")
    manager = get_download_manager(proxies if proxies else None, max_concurrent=50)
    manager.download_batch(url_list, path)


# ============ 以下为旧版接口，保持向后兼容 ============

async def download_file(
    url: str,
    path: Path,
    client,
    semaphore,
):
    """旧版 async 接口（不推荐使用）"""
    import httpx
    from pond.utils.crawler import get_mock_headers
    from urllib.parse import urlparse

    async with semaphore:
        try:
            response = await client.get(url, timeout=60, headers=get_mock_headers())

            if response.status_code == 200:
                logger.success(f"[{response.status_code}] {url}")

                file_path = path / urlparse(url).path[1:]
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(response.content)
            else:
                error_msg = f"[{response.status_code}] {url}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            logger.warning(f"Download failed (will retry): {url}, error: {e}")
            raise


async def async_tasks_with_client(
    url_list: list[str],
    path: Path,
    proxies: dict[str, str],
    max_concurrent: int = 10,
):
    """旧版 async 接口（不推荐使用）"""
    import asyncio

    import httpx

    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(
        proxies=proxies if proxies else None,
        limits=httpx.Limits(
            max_connections=max_concurrent,
            max_keepalive_connections=5,
        ),
    ) as client:
        tasks = [
            asyncio.create_task(download_file(url, path, client, semaphore))
            for url in url_list
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for url, result in zip(url_list, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {url}: {result}")


async def async_tasks(
    url_list: list[str],
    async_func: Callable,
    func_param_list: list[dict[str, Any]],
):
    """旧版接口（不推荐使用）"""
    import asyncio as _asyncio
    tasks = []
    for url in url_list:
        for func_params in func_param_list:
            task = _asyncio.create_task(async_func(url=url, **func_params))
            tasks.append(task)
    await _asyncio.gather(*tasks)


if __name__ == "__main__":
    url_list = [
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-05.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/DEFIUSDT/1m/DEFIUSDT-1m-2023-06.zip",
    ]

    start_async_download_files(url_list, Path("./temp"))
