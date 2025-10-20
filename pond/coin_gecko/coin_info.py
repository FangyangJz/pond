import requests
from loguru import logger


def get_coin_info(coingecko_id) -> dict:
    """从CoinGecko获取代币信息"""
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        logger.error(f"获取代币信息 {coingecko_id} 失败: {str(e)}")
        if str(e).startswith("404 Client Error: Not Found for url"):
            return {}
        return None


def get_coin_market_data(coingecko_id) -> dict:
    info = get_coin_info(coingecko_id)
    if not info:
        return None
    return info.get("market_data", {})


def get_coin_platforms(coingecko_id) -> dict:
    """从CoinGecko获取代币平台信息"""
    info = get_coin_info(coingecko_id)
    if not info:
        return None
    return info.get("platforms", {})
