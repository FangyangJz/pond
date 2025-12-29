import websocket
import threading
import json
from binance.client import Client
import polars as pl
from threading import Lock
from pond.utils.times import (
    utcstamp_mill2datetime,
)
from loguru import logger


class BinanceWebSocketClient:
    """Binance K线数据WebSocket客户端"""

    api_key = (None,)
    api_secret = (None,)

    def __init__(
        self,
        api_key=None,
        api_secret=None,
        symbols=["BTCUSDT"],
        interval=Client.KLINE_INTERVAL_1HOUR,
        proxy_host="127.0.0.1",
        proxy_port=7890,
        market_type="spot",  # 新增：市场类型，支持 "spot"（现货）或 "futures"（期货）
    ):
        """
        初始化WebSocket客户端
        :param symbols: 交易对列表
        :param interval: K线周期
        :param proxy_host: 代理主机
        :param proxy_port: 代理端口
        :param market_type: 市场类型 ("spot" 或 "futures")
        """
        self.api_key = api_key
        self.api_secret = api_secret
        # 确保symbols始终为列表格式
        if isinstance(symbols, str):
            self.symbols = [symbols]
        else:
            self.symbols = symbols
        self.interval = interval
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_url = f"http://{proxy_host}:{proxy_port}"
        self.ws = None
        self.ws_thread = None
        self.kline_data_store = []
        self.data_lock = Lock()
        self.market_type = market_type.lower()
        # 断线重连相关参数
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = -1  # 最大重连尝试次数
        self.reconnect_base_delay = 1  # 初始重连延迟(秒)

    def process_kline_data(self, kline_data):
        """
        处理推送的K线数据（核心逻辑：解析数据并更新本地K线）
        :param kline_data: WebSocket推送的原始数据（字典格式）
        """
        # 添加数据结构验证
        if not isinstance(kline_data, dict):
            print(f"无效的数据格式: {kline_data}")
            return

        # 检查是否包含必要的键
        required_keys = ["data"] if "stream" in kline_data else ["k"]
        for key in required_keys:
            if key not in kline_data:
                print(f"数据缺少必要字段 '{key}': {kline_data}")
                return

        # 1. 解析原始数据（处理不同格式的数据）
        if "stream" in kline_data:  # 组合流格式
            data = kline_data["data"]  # 提取组合流中的数据部分
            kline = data["k"]  # 核心K线数据在"data"字段的"k"子字段中
        else:  # 单一流格式
            kline = kline_data["k"]

        is_kline_closed = kline["x"]  # 是否是已收盘的K线（True=收盘，False=实时更新中）
        if not is_kline_closed:
            return  # 仅处理已收盘的K线
        event_time = utcstamp_mill2datetime(data["E"])

        # 2. 提取关键K线字段
        kline_info = {
            "pair": kline["s"],
            "interval": kline["i"],
            "event_time": event_time,
            "open_time": utcstamp_mill2datetime(kline["t"]),
            "close_time": utcstamp_mill2datetime(kline["T"]),
            "open": float(kline["o"]),
            "close": float(kline["c"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "volume": float(kline["v"]),
            "quote_volume": float(kline["q"]),
            "taker_buy_volume": float(kline["V"]),
            "taker_buy_quote_volume": float(kline["Q"]),
            "count": int(kline["n"]),
        }
        logger.debug(
            f"收到K线更新：{kline_info['pair']} {kline_info['interval']} close_time:{kline_info['close_time']}"
        )
        with self.data_lock:
            self.kline_data_store.append(kline_info)

    def start_kline_websocket(self):
        """启动WebSocket订阅多个K线"""
        # 复用批量订阅逻辑处理多标的
        self.start_batch_kline_subscription(self.interval, self.symbols)

    def reconnect(self):
        """使用指数退避策略进行重连尝试"""
        if (
            self.max_reconnect_attempts > 0
            and self.reconnect_attempts >= self.max_reconnect_attempts
        ):
            print(f"已达到最大重连尝试次数 ({self.max_reconnect_attempts})，停止尝试")
            return

        # 指数退避算法：delay = base_delay * (2 ^ attempts)
        delay = self.reconnect_base_delay * (2**self.reconnect_attempts)
        delay = min(delay, 60)  # 最大延迟为60秒
        print(f"计划第 {self.reconnect_attempts + 1} 次重连，延迟 {delay} 秒...")
        threading.Timer(delay, self.start_kline_websocket).start()

    def start_batch_kline_subscription(
        self, interval: str, symbols: list[str], testnet=False
    ):
        """
        批量订阅多个交易对的K线
        :param interval: K线周期
        :param symbols: 交易对列表
        """
        # 如果已有连接，先关闭
        if self.ws:
            self.ws.close()
            self.ws = None

        # 批量订阅：组合流格式：<交易对>@kline_<周期>
        streams = [f"{symbol.lower()}@kline_{interval.lower()}" for symbol in symbols]
        stream_path = "/stream?streams=" + "/".join(streams)

        # 根据市场类型选择WebSocket端点
        if self.market_type == "futures":
            base_url = (
                "wss://fstream.binance.com"
                if not testnet
                else "wss://testnet.binancefuture.com"
            )
        else:
            base_url = (
                "wss://stream.binance.com:9443"
                if not testnet
                else "wss://testnet.binance.vision"
            )

        # 创建WebSocket应用
        self.ws = websocket.WebSocketApp(
            f"{base_url}{stream_path}",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        # 在新线程中启动WebSocket连接
        self.ws_thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={
                "http_proxy_host": self.proxy_host,
                "http_proxy_port": self.proxy_port,
                "proxy_type": "http",
            },
        )
        self.ws_thread.start()

    def on_open(self, ws):
        """WebSocket连接打开时的回调"""
        print("WebSocket连接已打开")
        self.is_connected = True
        self.reconnect_attempts = 0  # 重置重连尝试次数

    def on_message(self, ws, message):
        """收到WebSocket消息时的回调"""
        try:
            data = json.loads(message)
            self.process_kline_data(data)
        except json.JSONDecodeError:
            print(f"无法解析WebSocket消息: {message}")

    def on_error(self, ws, error):
        """WebSocket错误时的回调"""
        print(f"WebSocket错误: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket关闭时的回调"""
        print(f"WebSocket连接已关闭: {close_status_code} - {close_msg}")
        self.is_connected = False

        # 触发重连逻辑
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect()

    def stop(self):
        """停止WebSocket连接"""
        if self.ws:
            self.ws.close()
            self.ws_thread.join()
            self.ws = None
            self.ws_thread = None
            self.is_connected = False
            self.reconnect_attempts = 0  # 重置重连状态
            print("WebSocket连接已停止")

    def get_kline_dataframe(self) -> pl.DataFrame:
        """
        将内存中的K线数据转换为Polars DataFrame

        :return: 包含所有K线数据的Polars DataFrame
        """
        with self.data_lock:  # 确保线程安全读取
            if not self.kline_data_store:
                return pl.DataFrame()

            # 转换为Polars DataFrame并按时间排序
            return pl.DataFrame(self.kline_data_store).sort("open_time")

    # 新增：清空内存数据
    def clear_kline_data(self):
        """清空内存中的K线数据"""
        with self.data_lock:
            self.kline_data_store.clear()


class BinanceWSClientWrapper:
    """管理多个BinanceWebSocketClient实例的包装类，实现标的分组订阅和数据聚合"""

    def __init__(
        self,
        api_key=None,
        api_secret=None,
        symbols=[],
        interval=Client.KLINE_INTERVAL_1HOUR,
        proxy_host="127.0.0.1",
        proxy_port=7890,
        market_type="spot",
        max_symbols_per_client=200,
    ):
        """
        初始化WebSocket客户端包装器
        :param symbols: 所有需要订阅的交易对列表
        :param interval: K线周期
        :param proxy_host: 代理主机
        :param proxy_port: 代理端口
        :param market_type: 市场类型
        :param max_symbols_per_client: 每个客户端最多订阅的交易对数量
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.interval = interval
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.market_type = market_type
        self.max_symbols_per_client = max_symbols_per_client
        self.clients: list[
            BinanceWebSocketClient
        ] = []  # 存储所有BinanceWebSocketClient实例
        self.clients_lock = Lock()  # 保护clients列表的线程锁
        self.data_lock = Lock()  # 保护聚合数据操作的线程锁

        # 将交易对分组
        self.symbol_groups = self._split_symbols_into_groups(symbols)
        # 为每个组创建客户端
        self._init_clients()

    def _split_symbols_into_groups(self, symbols):
        """将交易对列表拆分为多个组，每组不超过max_symbols_per_client个"""
        if not symbols:
            return []
        # 确保输入是列表
        if isinstance(symbols, str):
            symbols = [symbols]
        # 去重并排序（可选）
        unique_symbols = list(set(symbols))
        unique_symbols.sort()
        # 分组
        return [
            unique_symbols[i : i + self.max_symbols_per_client]
            for i in range(0, len(unique_symbols), self.max_symbols_per_client)
        ]

    def _init_clients(self):
        """为每个交易对组初始化一个BinanceWebSocketClient实例"""
        with self.clients_lock:
            self.clients = []
            for group in self.symbol_groups:
                client = BinanceWebSocketClient(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    symbols=group,
                    interval=self.interval,
                    proxy_host=self.proxy_host,
                    proxy_port=self.proxy_port,
                    market_type=self.market_type,
                )
                self.clients.append(client)

    def start_all(self):
        """启动所有WebSocket客户端"""
        with self.clients_lock:
            for i, client in enumerate(self.clients):
                print(
                    f"启动客户端 {i+1}/{len(self.clients)}，订阅 {len(client.symbols)} 个交易对"
                )
                client.start_kline_websocket()

    def stop_all(self):
        """停止所有WebSocket客户端"""
        with self.clients_lock:
            for i, client in enumerate(self.clients):
                print(f"停止客户端 {i+1}/{len(self.clients)}")
                client.stop()
            self.clients = []

    def get_aggregated_kline_dataframe(self) -> pl.DataFrame:
        """聚合所有客户端的K线数据并返回合并后的DataFrame"""
        with self.data_lock and self.clients_lock:
            if not self.clients:
                return pl.DataFrame()
            # 收集所有客户端的数据
            dataframes = [client.get_kline_dataframe() for client in self.clients]
            # 合并DataFrame
            combined_df = pl.concat(dataframes)
            # 按交易对和时间排序
            if not combined_df.is_empty():
                combined_df = combined_df.sort(by=["pair", "open_time"])
            return combined_df

    def clear_all_data(self):
        """清空所有客户端的K线数据"""
        with self.data_lock and self.clients_lock:
            for client in self.clients:
                client.clear_kline_data()

    def __del__(self):
        """析构函数，确保停止所有客户端"""
        self.stop_all()


if __name__ == "__main__":
    # 订阅期货K线（需确保代理支持期货域名访问）
    client = BinanceWebSocketClient(
        symbols=["BTCUSDT", "ETHUSDT"],
        interval=Client.KLINE_INTERVAL_1MINUTE,
        proxy_host="127.0.0.1",
        proxy_port=7890,
        market_type="futures",  # 指定为期货市场
    )
    client.start_kline_websocket()
    print("done")
