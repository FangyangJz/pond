import requests
from time import sleep
from loguru import logger


def get_coin_market_data(coingecko_id) -> dict:
    """从CoinGecko获取代币总供应量"""
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
    try:
        sleep(1)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("market_data", {})
    except Exception as e:
        logger.exception(e)
        return None
