import pandas as pd
from pond.utils.crawler import request_session


def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    from akshare.stock_feature.stock_hist_em import code_id_map_em

    code_id_dict = code_id_map_em()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }

    ses = request_session()
    r = ses.get(url, params=params)
    data_json = r.json()

    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()

    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ]
    temp_df.index = pd.to_datetime(temp_df["日期"])
    temp_df.reset_index(inplace=True, drop=True)

    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
    temp_df["最高"] = pd.to_numeric(temp_df["最高"])
    temp_df["最低"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])

    return temp_df


def stock_zt_pool_em(date: str = "20220426") -> pd.DataFrame:
    """
    东方财富网-行情中心-涨停板行情-涨停股池
    http://quote.eastmoney.com/ztb/detail#type=ztgc
    :return: 涨停股池
    :rtype: pandas.DataFrame
    """
    url = "http://push2ex.eastmoney.com/getTopicZTPool"
    params = {
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "dpt": "wz.ztzt",
        "Pageindex": "0",
        "pagesize": "10000",
        "sort": "fbt:asc",
        "date": date,
        "_": "1621590489736",
    }
    ses = request_session()
    r = ses.get(url, params=params)
    data_json = r.json()
    if data_json["data"] is None:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["pool"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = range(1, len(temp_df) + 1)
    temp_df.columns = [
        "序号",
        "代码",
        "_",
        "名称",
        "最新价",
        "涨跌幅",
        "成交额",
        "流通市值",
        "总市值",
        "换手率",
        "连板数",
        "首次封板时间",
        "最后封板时间",
        "封板资金",
        "炸板次数",
        "所属行业",
        "涨停统计",
    ]
    temp_df["涨停统计"] = (
        temp_df["涨停统计"].apply(lambda x: dict(x)["days"]).astype(str)
        + "/"
        + temp_df["涨停统计"].apply(lambda x: dict(x)["ct"]).astype(str)
    )
    temp_df = temp_df[
        [
            "序号",
            "代码",
            "名称",
            "涨跌幅",
            "最新价",
            "成交额",
            "流通市值",
            "总市值",
            "换手率",
            "封板资金",
            "首次封板时间",
            "最后封板时间",
            "炸板次数",
            "涨停统计",
            "连板数",
            "所属行业",
        ]
    ]
    temp_df["首次封板时间"] = temp_df["首次封板时间"].astype(str).str.zfill(6)
    temp_df["最后封板时间"] = temp_df["最后封板时间"].astype(str).str.zfill(6)
    temp_df["最新价"] = temp_df["最新价"] / 1000
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"])
    temp_df["总市值"] = pd.to_numeric(temp_df["总市值"])
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
    temp_df["封板资金"] = pd.to_numeric(temp_df["封板资金"])
    temp_df["炸板次数"] = pd.to_numeric(temp_df["炸板次数"])
    temp_df["连板数"] = pd.to_numeric(temp_df["连板数"])

    return temp_df


def stock_zh_a_spot_em() -> pd.DataFrame:
    """
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    """
    fields_dict = {
        "f2": "最新价",
        "f3": "涨跌幅",
        "f4": "涨跌额",
        "f5": "成交量",
        "f6": "成交额",
        "f7": "振幅",
        "f8": "换手率",
        "f9": "市盈率动",
        "f10": "量比",
        "f11": "5分钟涨跌",
        "f12": "代码",
        "f14": "名称",
        "f15": "最高",
        "f16": "最低",
        "f17": "今开",
        "f18": "昨收",
        "f20": "总市值",
        "f21": "流通市值",
        "f22": "涨速",
        "f23": "市净率",
        "f24": "60日涨跌幅",
        "f25": "年初至今涨跌幅",
        "f26": "上市时间",
        "f37": "加权净资产收益率",
        "f38": "总股本",
        "f39": "已流通股份",
        "f40": "营业收入",
        "f41": "营业收入同比增长",
        "f45": "归属净利润",
        "f46": "归属净利润同比增长",
        "f48": "每股未分配利润",
        "f49": "毛利率",
        "f57": "资产负债率",
        "f61": "每股公积金",
        "f100": "所处行业",
        "f112": "每股收益",
        "f113": "每股净资产",
        "f114": "市盈率静",
        "f115": "市盈率TTM",
        "f221": "报告期",
    }
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": ",".join(fields_dict.keys()),
        "_": "1623833739532",
    }
    ses = request_session()
    r = ses.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df.columns = fields_dict.values()

    for col in temp_df.columns:
        if col in ["名称", "代码", "所处行业"]:
            continue
        elif col in ["上市时间", "报告期"]:
            temp_df[col] = pd.to_datetime(
                temp_df[col], format="%Y%m%d", errors="coerce"
            )
        else:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

    # 数字开头的列名在dolphindb 查询的时候会有问题, 此处增加字母开头
    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
            "量比",
            "今开",
            "最高",
            "最低",
            "昨收",
            "涨速",
            "5分钟涨跌",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "市盈率动",
            "市盈率TTM",
            "市盈率静",
            "市净率",
            "每股收益",
            "每股净资产",
            "每股公积金",
            "每股未分配利润",
            "加权净资产收益率",
            "毛利率",
            "资产负债率",
            "营业收入",
            "营业收入同比增长",
            "归属净利润",
            "归属净利润同比增长",
            "报告期",
            "总股本",
            "已流通股份",
            "总市值",
            "流通市值",
            "所处行业",
            "上市时间",
        ]
    ].rename(columns={"5分钟涨跌": "A5分钟涨跌", "60日涨跌幅": "A60日涨跌幅"})

    return temp_df


if __name__ == "__main__":
    df = stock_zh_a_spot_em()
    df = stock_zt_pool_em()
    df = stock_zh_a_hist()
    print(1)
