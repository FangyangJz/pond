# !/usr/bin/env python3
# @Datetime : 2024/3/7 上午 01:02
# @Author   : Fangyang
# @Software : PyCharm

from typing import Any
import requests
import xmltodict
from loguru import logger

from pond.binance_history.exceptions import NetworkError


def get_vision_data_url_list(
    params: dict[str, Any], proxies: dict[str, str]
) -> tuple[bool, list[str]]:
    url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,ko;q=0.6",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    try:
        marker_str = f"&marker={params.get('marker')}" if "marker" in params else ""
        logger.info(
            f"Get {url}?delimiter={params.get('delimiter')}&prefix={params.get('prefix')}{marker_str}"
        )
        resp = requests.get(
            url,
            params=params,
            proxies=proxies,
            timeout=None,
            headers=headers,
        )
    except Exception as e:
        logger.error(url)
        raise NetworkError(e)

    if resp.status_code == 200:
        r = xmltodict.parse(resp.content.decode("utf-8"))["ListBucketResult"]
        is_truncated = True if r["IsTruncated"] == "true" else False

        if "Contents" in r:
            return is_truncated, [
                f"https://{r['Name']}/{e['Key']}"
                for e in r["Contents"]
                if "CHECKSUM" not in e["Key"]
            ]
        else:
            return False, []

    elif resp.status_code == 404:
        logger.warning(f"[404] {url}")
        return False, []
    else:
        raise NetworkError(resp.status_code)


if __name__ == "__main__":
    #### gdp
    # https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/BTCUSDT/1h/
    # https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/BTCUSDT/1h/&marker=data%2Ffutures%2Fum%2Fdaily%2Fklines%2FBTCUSDT%2F1h%2FBTCUSDT-1h-2021-05-13.zip.CHECKSUM
    a = "data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2021-05-13.zip.CHECKSUM"
    params = {"delimiter": r"/", "prefix": "data/futures/um/monthly/klines/BTCUSDT/1d/"}
    get_vision_data_url_list(
        params,
        proxies={
            # "http": "127.0.0.1:7890",
            # "https": "127.0.0.1:7890"
        },
    )
