import requests
from typing import Dict, List
import os

from enum import Enum


class ChainId(Enum):
    """区块链 Chain ID 枚举类，映射链名称与对应的 ID"""

    # 主流公链
    ETHEREUM = 1  # Ethereum 主网
    POLYGON = 137  # Polygon (Matic) 主网
    BSC = 56  # Binance Smart Chain 主网
    AVALANCHE = 43114  # Avalanche C 链主网
    ARBITRUM_ONE = 42161  # Arbitrum One 主网
    OPTIMISM = 10  # Optimism 主网
    BASE = 8453  # Base 主网
    ZKSYNC = 324  # zkSync Era 主网
    MERLIN = 4200  # Merlin 主网

    @classmethod
    def get_chain_id(cls, chain_name: str) -> int:
        """通过链名称（不区分大小写）获取对应的 Chain ID"""
        # 将输入的链名称转换为大写，匹配枚举的名称
        chain_name_upper = chain_name.strip().upper().replace(" ", "_")
        try:
            return cls[chain_name_upper].value
        except KeyError:
            raise ValueError(
                f"未找到链名称对应的 Chain ID: {chain_name}，请检查名称是否正确"
            )


class ChainbaseClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.chainbase.online"  # 注意域名和路径变化
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,  # 部分版本header键为X-API-Key（区分大小写）
        }

    def get_topn_holders(
        self, chain_id: int, contract_address: str, page=1, limit: int = 100
    ) -> List[Dict]:
        """查询代币持仓地址（使用最新端点）"""
        # 最新持仓查询端点为 /v1/token/holders
        url = f"{self.base_url}/v1/token/top-holders"
        params = {
            "chain_id": chain_id,
            "contract_address": contract_address,
            "page": page,
            "limit": limit,
            "order": "balance DESC",  # 按持仓量降序排列
        }

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json().get("data", [])
        except Exception as e:
            raise Exception(f"获取持仓数据失败: {str(e)}")


if __name__ == "__main__":
    API_KEY = os.environ.get("CHAIN_BASE_API_KEY", None)  # 替换为实际API密钥
    CHAIN_NAME = "ETHEREUM"  # 可使用名称（如"ethereum"）或符号（如"eth"）
    CONTRACT_ADDRESS = (
        "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # 示例：USDT合约地址
    )
    LIMIT = 10

    client = ChainbaseClient(API_KEY)

    try:
        # 获取Chain ID
        chain_id = ChainId.get_chain_id(CHAIN_NAME)
        print(f"区块链 {CHAIN_NAME} 的ID为: {chain_id}")

        # 查询持仓数据
        holders = client.get_topn_holders(
            chain_id, CONTRACT_ADDRESS, page=1, limit=LIMIT
        )
        print(f"\n前{len(holders)}个持仓地址:")
        for idx, holder in enumerate(holders[:5], 1):  # 只打印前5条示例
            print(
                f"{idx}. 地址: {holder['wallet_address']}, 持仓量: {holder['amount']}"
            )
        print("...（省略后续地址）")

    except Exception as e:
        print(f"错误: {str(e)}")
