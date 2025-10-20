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
            pass
        return None
