import json
import os
from datetime import datetime, timedelta
from pond.coin_gecko.coin_info import get_coin_platforms


# 2. 带文件缓存的币安合约信息工具
class BinanceContractTool:
    def __init__(self, cache_file="token_chain_cache.json", cache_expiry_days=10):
        """
        初始化工具类
        :param cache_file: 缓存文件路径
        :param cache_expiry_days: 缓存有效期（天）
        """
        self.binance_api_base = "https://fapi.binance.com"
        self.coingecko_api_base = "https://api.coingecko.com/api/v3"
        self.cache_file = cache_file
        self.cache_expiry_days = cache_expiry_days
        self._load_cache()  # 加载已有缓存

    def _load_cache(self):
        """加载本地缓存文件"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
            except Exception as e:
                print(f"加载缓存失败，将创建新缓存: {e}")
                self.cache = {}
        else:
            self.cache = {}

    def _save_cache(self):
        """保存缓存到文件"""
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存缓存失败: {e}")

    def _is_cache_valid(self, cache_entry):
        """检查缓存是否有效（未过期）"""
        if not cache_entry:
            return False
        cache_time = datetime.fromisoformat(cache_entry["cache_time"])
        return datetime.now() - cache_time <= timedelta(days=self.cache_expiry_days)

    def get_token_chain_info(self, token_symbol: str) -> dict:
        """
        获取代币链信息（优先从缓存读取，缓存失效则重新获取）
        :return: {chain_name, chain_id, contract_address, cache_time}
        """
        # 检查缓存
        cache_key = token_symbol.lower()
        if cache_key in self.cache:
            cache_entry = self.cache[cache_key]
            if self._is_cache_valid(cache_entry):
                print(f"使用缓存数据: {token_symbol}")
                return cache_entry["platforms"]
            else:
                print(f"缓存过期，重新获取: {token_symbol}")

        # 缓存无效，重新获取数据
        platforms = get_coin_platforms(token_symbol)
        if platforms is None:
            return None
        if len(platforms) > 0:
            # 存入缓存（添加时间戳）
            self.cache[cache_key] = {
                "platforms": platforms,
                "cache_time": datetime.now().isoformat(),  # 记录缓存时间
            }
            self._save_cache()  # 保存到文件
        return platforms


# 使用示例
if __name__ == "__main__":
    # 初始化工具（缓存文件默认token_chain_cache.json，有效期7天）
    tool = BinanceContractTool(cache_expiry_days=3)  # 可自定义有效期

    # 示例1：查询USDTUSDT合约的标的资产信息
    contract_symbol = "COAIUSDT"
    try:
        underlying = "swipe"
        print(f"合约 {contract_symbol} 标的资产: {underlying}")

        # 第一次查询（无缓存，会请求API并保存缓存）
        info = tool.get_token_chain_info(underlying)
        print(f"链信息: {info}")

        # 第二次查询（使用缓存）
        info_cached = tool.get_token_chain_info(underlying)
        print(f"缓存的链信息: {info_cached}")

    except Exception as e:
        print(f"错误: {e}")

    # 示例2：查询其他代币（如AAVE）
    try:
        aave_info = tool.get_token_chain_info("AAVE")
        print(f"\nAAVE链信息: {aave_info}")
    except Exception as e:
        print(f"错误: {e}")
