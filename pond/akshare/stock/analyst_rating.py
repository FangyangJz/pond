import json
import threading

from typing import Any, Dict, List

import pandas as pd
from datetime import datetime

from pond.utils.code2code import trans_to_juejin_code
from pond.utils.crawler import request_session


def request_data(
        url: str, num: int, params: Dict[str, Any],
        # headers: Dict[str, str],
        columns: List[str],
        res_dict: Dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    ses = request_session()
    r = ses.get(url=url, params=params)
    # r = requests.get(url, params=params, headers=headers, timeout=5)

    r.encoding = r.apparent_encoding

    txt = r.text
    if "[]" in txt:
        print("页面已经到头")
        res_dict['end'] = pd.DataFrame()
        return res_dict

    txt = json.loads(txt)
    rate_name_list = txt['data']

    res_dict[f"{num}"] = pd.DataFrame.from_records(rate_name_list)[columns]

    return res_dict


def get_stock_analyst_rating(
        start: str = "2005-01-01",
        end: str = datetime.now().date().strftime("%Y-%m-%d")
):
    """
     点击翻页1就能看到url
    "https://data.eastmoney.com/report/stock.jshtml"
    "https://www.bilibili.com/video/BV1xT411c775/?vd_source=ac309ed33e549fafe9293cd08a468cd4"
    :return:
    """

    num = 1
    all_data = pd.DataFrame()
    url = "https://reportapi.eastmoney.com/report/list"

    columns = [
        'title', 'stockName', 'stockCode', 'orgSName', 'publishDate',
        # 'orgCode', 'orgName', 'infoCode','column',
        'predictNextTwoYearEps', 'predictNextTwoYearPe',
        'predictNextYearEps', 'predictNextYearPe',
        'predictThisYearEps', 'predictThisYearPe',
        # 'predictLastYearEps', 'predictLastYearPe',
        # 'actualLastTwoYearEps', 'actualLastYearEps',
        # 'industryCode', 'industryName', 'emIndustryCode',
        'indvInduCode', 'indvInduName',
        # 'emRatingCode', 'emRatingValue', 'lastEmRatingCode', 'lastEmRatingValue',
        'emRatingName', 'lastEmRatingName',
        # 'ratingChange', 'reportType', 'indvIsNew', 'author',
        'researcher',
        # 'newListingDate', 'newPurchaseDate', 'newIssuePrice', 'newPeIssueA',
        'indvAimPriceT', 'indvAimPriceL',
        # 'attachType', 'attachSize', 'attachPages', 'encodeUrl',
        'sRatingName',
        # 'sRatingCode',
        'market', 'authorID', 'count',
        # 'orgType'
    ]

    res_dict = dict()

    t_list = []
    while True:
        print(f"[{num}]" + "=" * 100)
        params = {
            "industryCode": "*",
            "pageSize": "100",
            "industry": "*",
            "rating": "",
            "ratingChange": "",
            "beginTime": start,
            "endTime": end,
            "pageNo": f"{num}",
            "fields": "",
            "qType": "0",
            "orgCode": "",
            "code": "*",
            "rcode": "",
            "p": f"{num}",
            "pageNum": f"{num}",
            "pageNumber": f"{num}",
            "_": "1660628644570"
        }

        # request_data(url, num, params, columns, res_dict)
        t = threading.Thread(target=request_data, args=(url, num, params, columns, res_dict))
        t.start()
        t_list.append(t)

        num += 1
        if num % 100 == 0:
            [t.join() for t in t_list]
            t_list = []
            if "end" in res_dict:
                break

    del res_dict['end']

    all_data = pd.concat(list(res_dict.values())) \
        .rename(columns={'publishDate': "trade_date"}) \
        .sort_values(by="trade_date", ascending=False).reset_index(drop=True)

    all_data["jj_code"] = all_data['stockCode'].apply(trans_to_juejin_code)
    all_data['authorID'] = all_data['authorID'].apply(lambda x: str(x))

    return all_data


if __name__ == '__main__':
    import time

    start_time = time.perf_counter()
    df:pd.DataFrame = get_stock_analyst_rating()
    print(f"Time cost: {time.perf_counter() - start_time:.2f}s")
    df.to_pickle("pingji.pkl")

    df = pd.read_pickle('pingji.pkl')\
        .rename(columns={'publishDate': "trade_date"})\
        .sort_values(by="trade_date", ascending=False)\
        .reset_index(drop=True)
    df["jj_code"] = df['stockCode'].apply(trans_to_juejin_code)
    df['authorID'] = df['authorID'].apply(lambda x: str(x))
    print(1)
