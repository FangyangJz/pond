# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/19 23:01
# @Author   : Fangyang
# @Software : PyCharm


f041_054_current_liabilities = {
    # 2.2.1 流动负债
    '041短期借款': 'FFF041',
    '042交易性金融负债': 'FFF042',
    '043应付票据': 'FFF043',
    '044应付账款': 'FFF044',
    '045预收款项': 'FFF045',
    '046应付职工薪酬': 'FFF046',
    '047应交税费': 'FFF047',
    '048应付利息': 'FFF048',
    '049应付股利': 'FFF049',
    '050其他应付款': 'FFF050',
    '051应付关联公司款': 'FFF051',
    '052一年内到期的非流动负债': 'FFF052',
    '053其他流动负债': 'FFF053',
    '054流动负债合计': 'FFF054',
}

f055_062_noncurrent_liabilities_dict = {
    # 2.2.2 非流动负债
    '055长期借款': 'FFF055',
    '056应付债券': 'FFF056',
    '057长期应付款': 'FFF057',
    '058专项应付款': 'FFF058',
    '059预计负债': 'FFF059',
    '060递延所得税负债': 'FFF060',
    '061其他非流动负债': 'FFF061',
    '062非流动负债合计': 'FFF062',
}

f041_063_liabilities_dict = {
    # 2.2 负债
    **f041_054_current_liabilities,
    **f055_062_noncurrent_liabilities_dict,
    '063负债合计': 'FFF063',
}

if __name__ == '__main__':
    pass
