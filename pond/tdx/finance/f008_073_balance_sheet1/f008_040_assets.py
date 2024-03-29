# !/usr/bin/env python3

# @Datetime : 2020/10/19 23:00
# @Author   : Fangyang
# @Software : PyCharm


f008_021_current_assets_dict = {
    # 2.1.1 流动资产
    "008货币资金": "FFF008",
    "009交易性金融资产": "FFF009",
    "010应收票据": "FFF010",
    "011应收账款": "FFF011",
    "012预付款项": "FFF012",
    "013其他应收款": "FFF013",
    "014应收关联公司款": "FFF014",
    "015应收利息": "FFF015",
    "016应收股利": "FFF016",
    "017存货": "FFF017",
    "018其中：消耗性生物资产": "FFF018",
    "019一年内到期的非流动资产": "FFF019",
    "020其他流动资产": "FFF020",
    "021流动资产合计": "FFF021",
}

f022_039_noncurrent_assets_dict = {
    # 2.1.2 非流动资产
    "022可供出售金融资产": "FFF022",
    "023持有至到期投资": "FFF023",
    "024长期应收款": "FFF024",
    "025长期股权投资": "FFF025",
    "026投资性房地产": "FFF026",
    "027固定资产": "FFF027",
    "028在建工程": "FFF028",
    "029工程物资": "FFF029",
    "030固定资产清理": "FFF030",
    "031生产性生物资产": "FFF031",
    "032油气资产": "FFF032",
    "033无形资产": "FFF033",
    "034开发支出": "FFF034",
    "035商誉": "FFF035",
    "036长期待摊费用": "FFF036",
    "037递延所得税资产": "FFF037",
    "038其他非流动资产": "FFF038",
    "039非流动资产合计": "FFF039",
}

f008_040_assets_dict = {
    # 2.1 资产
    # 2.1.1 流动资产
    **f008_021_current_assets_dict,
    # 2.1.2 非流动资产
    **f022_039_noncurrent_assets_dict,
    "040资产总计": "FFF040",
}

if __name__ == "__main__":
    pass
