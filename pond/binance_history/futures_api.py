from binance.client import Client
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger


def get_long_short_account_ratio_history(
    client: Client, symbol, period, start_time: datetime, end_time: datetime
):
    start_time = max(start_time, datetime.now() - timedelta(days=29))
    params = {"symbol": symbol, "period": period, "limit": 1000}
    params["startTime"] = int(start_time.timestamp() * 1000)
    params["endTime"] = int(end_time.timestamp() * 1000)
    try:
        data = client.futures_top_longshort_account_ratio(**params)
        if not data or len(data) == 0:
            return []
        df = pd.DataFrame(data=data)
        df["close_time"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["jj_code"] = symbol
        return df
    except Exception as e:
        logger.warning(f"failed for {symbol}, {e}")
    return None


def get_open_interest_history(
    client: Client,
    symbol,
    period,
    start_time: datetime,
    end_time: datetime,
    limit_per_request=1000,
):
    start_time = max(start_time, datetime.now() - timedelta(days=29))
    try:
        params = {
            "symbol": symbol,
            "period": period,
            "limit": limit_per_request,
        }
        params["startTime"] = int(start_time.timestamp() * 1000)
        params["endTime"] = int(end_time.timestamp() * 1000)
        data = client.futures_open_interest_hist(**params)
        if not data or len(data) == 0:
            return []
        df = pd.DataFrame(data)
        df["close_time"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["jj_code"] = symbol
        return df
    except Exception as e:
        logger.warning(f"failed for {symbol}, {e}")

    return None
