import requests
import json
from pathlib import Path


class CoinGeckoIDMapper:
    def __init__(self, cache_file="coingecko_precise_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()  # 缓存已查询过的结果，避免重复请求

    def _load_cache(self):
        """加载本地缓存（存储已查询成功的映射）"""
        if Path(self.cache_file).exists():
            with open(self.cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        """保存查询结果到缓存"""
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def get_coingecko_id(self, query, exact_match=True):
        """
        精确查询合约名称/符号对应的coingecko_id
        query: 合约名称（如"Binance Coin"）或符号（如"BNB"）
        exact_match: 是否开启精确匹配（True=严格匹配名称或符号，False=模糊匹配）
        """
        # 先查缓存，命中则直接返回
        query_lower = query.lower()
        if query_lower in self.cache:
            return self.cache[query_lower]

        # 调用CoinGecko搜索API
        url = "https://api.coingecko.com/api/v3/search"
        params = {"query": query}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            results = response.json().get("coins", [])  # 搜索结果列表

            if not results:
                return None  # 无匹配结果

            # 处理查询结果
            query_normalized = query.lower().strip()
            for coin in results:
                # 提取名称和符号并标准化
                name = coin["name"].lower().strip()
                symbol = coin["symbol"].lower().strip()

                # 精确匹配：名称或符号完全一致
                if exact_match:
                    if name == query_normalized or symbol == query_normalized:
                        coingecko_id = coin["id"]
                        self.cache[query_lower] = coingecko_id  # 存入缓存
                        self._save_cache()
                        return coingecko_id
                # 模糊匹配：名称或符号包含查询词
                else:
                    if query_normalized in name or query_normalized in symbol:
                        coingecko_id = coin["id"]
                        self.cache[query_lower] = coingecko_id  # 存入缓存
                        self._save_cache()
                        return coingecko_id

            # 无精确匹配结果
            return None

        except Exception as e:
            print(f"查询失败：{str(e)}")
            return None


# 示例用法
if __name__ == "__main__":
    mapper = CoinGeckoIDMapper()

    # 测试精确匹配（推荐）
    test_queries = [
        "BTC",  # 符号（精确匹配）
        "ethereum",  # 名称（精确匹配）
        "BNB",  # 全称（精确匹配）
        "SOL",  # 符号（精确匹配）e:\workspace\quant\CryptoTradeSimple\report\misc\coingecko_tokens_cache.json
        "invalid token",  # 无效查询
    ]

    for query in test_queries:
        cg_id = mapper.get_coingecko_id(query, exact_match=True)
        print(f"{query} → coingecko_id: {cg_id}")

    # 测试模糊匹配（适用于名称不完全准确的情况）
    print("\n模糊匹配示例：")
    print(f"bit → {mapper.get_coingecko_id('bit', exact_match=False)}")  # 匹配bitcoin
    print(
        f"binance → {mapper.get_coingecko_id('binance', exact_match=False)}"
    )  # 匹配binancecoin
