# !/usr/bin/env python3
"""
高效并发下载管理器

设计原则:
1. 全局共享 httpx.Client - 复用连接池
2. 全局共享 ThreadPoolExecutor - 避免线程重复创建
3. 全局并发控制 - 防止过载
4. 线程安全 - 支持多线程调用
"""
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional
import atexit
import threading
from concurrent.futures import ThreadPoolExecutor, Future
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from pond.utils.crawler import get_mock_headers


class DownloadManager:
    """
    单例模式的下载管理器，提供全局共享的下载资源。

    使用方式:
        manager = DownloadManager.get_instance(proxies, max_concurrent=50)
        manager.download_batch(url_list, path)
        # 或者在程序结束时
        manager.shutdown()
    """

    _instance: Optional["DownloadManager"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        raise RuntimeError("Use get_instance() to get the singleton instance")

    @classmethod
    def get_instance(
        cls,
        proxies: dict[str, str] | None = None,
        max_concurrent: int = 50,
    ) -> "DownloadManager":
        """获取单例实例，线程安全。"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = object.__new__(cls)
                    instance._init(proxies, max_concurrent)
                    cls._instance = instance
        return cls._instance

    @classmethod
    def reset_instance(cls):
        """重置单例（用于测试或重新配置）。"""
        with cls._lock:
            if cls._instance is not None:
                instance = cls._instance
                cls._instance = None
                # 取消注册 atexit handler，手动关闭
                try:
                    atexit.unregister(instance._cleanup_on_exit)
                except Exception:
                    pass
                instance.shutdown()

    def _init(self, proxies: dict[str, str] | None, max_concurrent: int):
        """初始化共享资源。"""
        self.proxies = proxies or {}
        self.max_concurrent = max_concurrent
        self._shutdown = False

        # 共享的 HTTP 客户端，连接池复用
        self.client = httpx.Client(
            proxies=self.proxies if self.proxies else None,
            limits=httpx.Limits(
                max_connections=max_concurrent,
                max_keepalive_connections=max_concurrent // 2,
            ),
            timeout=httpx.Timeout(60.0, connect=10.0),
            http2=True,  # 启用 HTTP/2 多路复用
        )

        # 共享的线程池 (daemon=True 让程序可以正常退出)
        self.executor = ThreadPoolExecutor(
            max_workers=max_concurrent,
            thread_name_prefix="download_worker",
        )

        # 用于跟踪下载任务的锁
        self._pending_futures: dict[Future, str] = {}
        self._futures_lock = threading.Lock()

        # 注册退出时的清理函数
        atexit.register(self._cleanup_on_exit)

        logger.info(
            f"DownloadManager initialized: max_concurrent={max_concurrent}, "
            f"http2=True, proxies={'enabled' if self.proxies else 'disabled'}"
        )

    def _cleanup_on_exit(self):
        """程序退出时自动清理资源。"""
        if not self._shutdown:
            self.shutdown(wait=False)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _download_single(self, url: str, path: Path) -> str:
        """
        下载单个文件（内部方法，带重试）。

        Returns:
            下载的文件路径字符串

        Raises:
            Exception: 下载失败时抛出
        """
        response = self.client.get(url, headers=get_mock_headers())

        if response.status_code == 200:
            file_path = path / urlparse(url).path[1:]
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(response.content)
            logger.success(f"[200] {url}")
            return str(file_path)
        else:
            raise Exception(f"HTTP {response.status_code}: {url}")

    def download(self, url: str, path: Path) -> Future:
        """
        提交单个下载任务（非阻塞）。

        Returns:
            Future 对象，可通过 future.result() 获取结果
        """
        if self._shutdown:
            raise RuntimeError("DownloadManager has been shut down")

        future = self.executor.submit(self._download_single, url, path)
        with self._futures_lock:
            self._pending_futures[future] = url
            future.add_done_callback(self._on_download_done)
        return future

    def _on_download_done(self, future: Future):
        """下载完成回调。"""
        with self._futures_lock:
            url = self._pending_futures.pop(future, None)
        if url:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Failed to download {url}: {e}")

    def download_batch(
        self,
        url_list: list[str],
        path: Path,
        fail_fast: bool = False,
    ) -> tuple[int, int]:
        """
        批量下载文件（阻塞，等待所有完成）。

        Args:
            url_list: URL 列表
            path: 保存根目录
            fail_fast: 是否在第一个失败时立即抛出异常

        Returns:
            (成功数, 失败数)
        """
        if not url_list:
            return (0, 0)

        logger.info(f"Downloading {len(url_list)} files...")

        futures = [self.download(url, path) for url in url_list]

        success_count = 0
        fail_count = 0
        first_error = None

        for future in futures:
            try:
                future.result()
                success_count += 1
            except Exception as e:
                fail_count += 1
                if first_error is None:
                    first_error = e
                if fail_fast:
                    raise e

        if fail_count > 0:
            logger.warning(f"Download batch completed: {success_count} success, {fail_count} failed")
        else:
            logger.success(f"Download batch completed: {success_count} files")

        if first_error and fail_fast:
            raise first_error

        return (success_count, fail_count)

    def download_batch_async(
        self,
        url_list: list[str],
        path: Path,
    ) -> list[Future]:
        """
        批量提交下载任务（非阻塞）。

        Returns:
            Future 列表，调用者需自行等待
        """
        return [self.download(url, path) for url in url_list]

    def wait_all(self):
        """等待所有待处理的下载任务完成。"""
        with self._futures_lock:
            futures = list(self._pending_futures.keys())
        for future in futures:
            try:
                future.result()
            except Exception:
                pass  # 错误已在回调中记录

    def shutdown(self, wait: bool = True):
        """关闭下载管理器，释放资源。"""
        if self._shutdown:
            return
        self._shutdown = True

        logger.info("Shutting down DownloadManager...")
        if wait:
            self.wait_all()
        self.executor.shutdown(wait=wait, cancel_futures=not wait)
        self.client.close()
        logger.info("DownloadManager shut down complete")


# 便捷函数，保持向后兼容
_global_manager: Optional[DownloadManager] = None


def get_download_manager(
    proxies: dict[str, str] | None = None,
    max_concurrent: int = 50,
) -> DownloadManager:
    """获取全局下载管理器。"""
    global _global_manager
    if _global_manager is None:
        _global_manager = DownloadManager.get_instance(proxies, max_concurrent)
    return _global_manager


def start_async_download_files(
    url_list: list[str],
    path: Path,
    proxies: dict[str, str] = {},
    max_concurrent: int = 10,  # 这个参数现在被忽略，使用全局配置
):
    """
    兼容旧接口的下载函数。

    注意: max_concurrent 参数现在被忽略，使用全局 DownloadManager 的配置。
    """
    if not url_list:
        return

    manager = get_download_manager(proxies or None, max_concurrent=50)
    manager.download_batch(url_list, path)


if __name__ == "__main__":
    # 测试
    manager = DownloadManager.get_instance(max_concurrent=10)

    url_list = [
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2023-01.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2023-02.zip",
        "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2023-03.zip",
    ]

    success, fail = manager.download_batch(url_list, Path("./temp"))
    print(f"Result: {success} success, {fail} failed")

    manager.shutdown()
