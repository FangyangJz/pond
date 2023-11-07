# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/19 22:59
# @Author   : Fangyang
# @Software : PyCharm


from gulf.tdx.finance.f008_073_balance_sheet1.f008_040_assets import f008_040_assets_dict
from gulf.tdx.finance.f008_073_balance_sheet1.f064_072_equity import f064_072_owner_equity_dict
from gulf.tdx.finance.f008_073_balance_sheet1.f041_063_liabilities import f041_063_liabilities_dict


f008_073_balance_sheet_dict = {
    # 2. 资产负债表 BALANCE SHEET
    # 2.1 资产
    **f008_040_assets_dict,
    # 2.2 负债
    **f041_063_liabilities_dict,
    # 2.3 所有者权益
    **f064_072_owner_equity_dict,
    '073负债和所有者（或股东权益）合计': 'FFF073',
}

if __name__ == '__main__':
    pass
