# !/usr/bin/env python3
# @Datetime : 2023/10/26 23:57
# @Author   : Fangyang
# @Software : PyCharm

from gulf.tdx.finance.f008_073_balance_sheet1 import f008_073_balance_sheet_dict
from gulf.tdx.finance.f108_158_cash_flow_statement1 import (
    f098_158_cash_flow_statement_dict,
)
from gulf.tdx.finance.f159_229_fundamental_analysis import (
    f159_229_fundamental_analysis_dict,
)
from gulf.tdx.finance.f074_097_income_statement1 import f074_097_income_statement_dict
from gulf.tdx.finance.f401_439_balance_sheet2 import f401_439_balance_sheet_dict
from gulf.tdx.finance.f501_521_income_statement2 import f501_521_income_statement_dict
from gulf.tdx.finance.f561_580_cash_flow_statement2 import (
    f561_580_cash_flow_statement_dict,
)

id_dict = {
    "000股票代码": "code",
    "000报告日期": "trade_date",
}

f001_007_info_per_share_dict = {
    # 1.每股指标
    "001基本每股收益": "FFF001",  # EPS
    "002扣除非经常性损益每股收益": "FFF002",
    "003每股未分配利润": "FFF003",
    "004每股净资产": "FFF004",
    "005每股资本公积金": "FFF005",
    "006净资产收益率": "FFF006",  # ROE
    "007每股经营现金流量": "FFF007",
}

financial_dict = {
    # 1.每股指标 0~7
    **f001_007_info_per_share_dict,
    # 资产负债表 8~73
    **f008_073_balance_sheet_dict,
    # 利润表 74~97
    **f074_097_income_statement_dict,
    # 现金流量表 98~158
    **f098_158_cash_flow_statement_dict,
    # 基本面分析 159~229
    **f159_229_fundamental_analysis_dict,
    # 11. 单季度财务指标
    "230营业收入": "FFF230",
    "231营业利润": "FFF231",
    "232归属于母公司所有者的净利润": "FFF232",
    "233扣除非经常性损益后的净利润": "FFF233",
    "234经营活动产生的现金流量净额": "FFF234",
    "235投资活动产生的现金流量净额": "FFF235",
    "236筹资活动产生的现金流量净额": "FFF236",
    "237现金及现金等价物净增加额": "FFF237",
    # 12.股本股东
    "238总股本": "FFF238",
    "239已上市流通A股": "FFF239",
    "240已上市流通B股": "FFF240",
    "241已上市流通H股": "FFF241",
    "242股东人数(户)": "FFF242",
    "243第一大股东的持股数量": "FFF243",
    "244十大流通股东持股数量合计(股)": "FFF244",
    "245十大股东持股数量合计(股)": "FFF245",
    # 13.机构持股
    "246机构总量(家)": "FFF246",
    "247机构持股总量(股)": "FFF247",
    "248QFII机构数": "FFF248",
    "249QFII持股量": "FFF249",
    "250券商机构数": "FFF250",
    "251券商持股量": "FFF251",
    "252保险机构数": "FFF252",
    "253保险持股量": "FFF253",
    "254基金机构数": "FFF254",
    "255基金持股量": "FFF255",
    "256社保机构数": "FFF256",
    "257社保持股量": "FFF257",
    "258私募机构数": "FFF258",
    "259私募持股量": "FFF259",
    "260财务公司机构数": "FFF260",
    "261财务公司持股量": "FFF261",
    "262年金机构数": "FFF262",
    "263年金持股量": "FFF263",
    # 14.新增指标
    # [注：季度报告中，若股东同时持有非流通A股性质的股份(如同时持有流通A股和流通B股)，取的是包含同时持有非流通A股性质的流通股数]
    "264十大流通股东中持有A股合计(股)": "FFF264",
    "265第一大流通股东持股量(股)": "FFF265",
    # 1.自由流通股=已流通A股-十大流通股东5%以上的A股；
    # 2.季度报告中，若股东同时持有非流通A股性质的股份(如同时持有流通A股和流通H股)，5%以上的持股取的是不包含同时持有非流通A股性质的流通股数，结果可能偏大；
    # 3.指标按报告期展示，新股在上市日的下个报告期才有数据
    "266自由流通股(股)": "FFF266",
    "267受限流通A股(股)": "FFF267",
    "268一般风险准备(金融类)": "FFF268",
    "269其他综合收益(利润表)": "FFF269",
    "270综合收益总额(利润表)": "FFF270",
    "271归属于母公司股东权益(资产负债表)": "FFF271",
    "272银行机构数(家)(机构持股)": "FFF272",
    "273银行持股量(股)(机构持股)": "FFF273",
    "274一般法人机构数(家)(机构持股)": "FFF274",
    "275一般法人持股量(股)(机构持股)": "FFF275",
    "276近一年净利润(元)": "FFF276",
    "277信托机构数(家)(机构持股)": "FFF277",
    "278信托持股量(股)(机构持股)": "FFF278",
    "279特殊法人机构数(家)(机构持股)": "FFF279",
    "280特殊法人持股量(股)(机构持股)": "FFF280",
    "281加权净资产收益率(每股指标)": "FFF281",
    "282扣非每股收益(单季度财务指标)": "FFF282",
    "283最近一年营业收入(万元)": "FFF283",
    "284国家队持股数量(万股)": "FFF284",
    # [注：本指标统计包含汇金公司、证金公司、外汇管理局旗下投资平台、国家队基金、国开、养老金以及中科汇通等国家队机构持股数量]
    "285业绩预告-本期净利润同比增幅下限%": "FFF285",
    # [注：指标285至294展示未来一个报告期的数据。例，3月31日至6月29日这段时间内展示的是中报的数据；如果最新的财务报告后面有多个报告期的业绩预告/快报，只能展示最新的财务报告后面的一个报告期的业绩预告/快报]
    "286业绩预告-本期净利润同比增幅上限%": "FFF286",
    "287业绩快报-归母净利润": "FFF287",
    "288业绩快报-扣非净利润": "FFF288",
    "289业绩快报-总资产": "FFF289",
    "290业绩快报-净资产": "FFF290",
    "291业绩快报-每股收益": "FFF291",
    "292业绩快报-摊薄净资产收益率": "FFF292",
    "293业绩快报-加权净资产收益率": "FFF293",
    "294业绩快报-每股净资产": "FFF294",
    "295应付票据及应付账款(资产负债表)": "FFF295",
    "296应收票据及应收账款(资产负债表)": "FFF296",
    "297递延收益(资产负债表)": "FFF297",
    "298其他综合收益(资产负债表)": "FFF298",
    "299其他权益工具(资产负债表)": "FFF299",
    "300其他收益(利润表)": "FFF300",
    "301资产处置收益(利润表)": "FFF301",
    "302持续经营净利润(利润表)": "FFF302",
    "303终止经营净利润(利润表)": "FFF303",
    "304研发费用(利润表)": "FFF304",
    "305其中:利息费用(利润表-财务费用)": "FFF305",
    "306其中:利息收入(利润表-财务费用)": "FFF306",
    "307近一年经营活动现金流净额": "FFF307",
    "308近一年归母净利润(万元)": "FFF308",
    "309近一年扣非净利润(万元)": "FFF309",
    "310近一年现金净流量(万元)": "FFF310",
    "311基本每股收益(单季度)": "FFF311",
    "312营业总收入(单季度)(万元)": "FFF312",
    # [注：本指标展示未来一个报告期的数据。例,3月31日至6月29日这段时间内展示的是中报的数据；
    # 如果最新的财务报告后面有多个报告期的业绩预告/快报，只能展示最新的财务报告后面的一个报告期的业绩预告/快报的数据；
    # 公告日期格式为YYMMDD，例：190101代表2019年1月1日]
    "313业绩预告公告日期": "FFF313",
    # [注：日期格式为YYMMDD, 例：190101代表2019年1月1日]
    "314财报公告日期": "FFF314",
    "315业绩快报公告日期": "FFF315",
    # [注：本指标展示未来一个报告期的数据。例,3月31日至6月29日这段时间内展示的是中报的数据；
    # 如果最新的财务报告后面有多个报告期的业绩预告/快报，只能展示最新的财务报告后面的一个报告期的业绩预告/快报的数据；
    # 公告日期格式为YYMMDD，例：190101代表2019年1月1日]
    "316近一年投资活动现金流净额(万元)": "FFF316",
    # [注：指标317至318展示未来一个报告期的数据。例，3月31日至6月29日这段时间内展示的是中报的数据；
    # 如果最新的财务报告后面有多个报告期的业绩预告/快报，只能展示最新的财务报告后面的一个报告期的业绩预告/快报]
    "317业绩预告-本期净利润下限(万元)": "FFF317",
    "318业绩预告-本期净利润上限(万元)": "FFF318",
    "319营业总收入TTM(万元)": "FFF319",
    "320员工总数(人)": "FFF320",
    "321每股企业自由现金流": "FFF321",
    "322每股股东自由现金流": "FFF322",
    **f401_439_balance_sheet_dict,
    **f501_521_income_statement_dict,
    **f561_580_cash_flow_statement_dict,
}

no_use_dict = {
    "192": "FFF192",
    **{f"{i}": f"FFF{i}" for i in range(323, 401)},
    **{f"{i}": f"FFF{i}" for i in range(440, 501)},
    **{f"{i}": f"FFF{i}" for i in range(522, 561)},
}

if __name__ == "__main__":
    # d1 = {f'{i}': f'FFF{i}' for i in range(440, 501)}

    print(1)
