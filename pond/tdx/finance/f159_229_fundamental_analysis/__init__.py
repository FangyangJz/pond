# !/usr/bin/env python3
# -*- coding:utf-8 -*-

# @Datetime : 2020/10/20 2:19
# @Author   : Fangyang
# @Software : PyCharm

from gulf.tdx.finance.f159_229_fundamental_analysis.f159_171_solvency import f159_171_solvency_dict
from gulf.tdx.finance.f159_229_fundamental_analysis.f172_182_operating_capability import f172_182_operating_capability_dict
from gulf.tdx.finance.f159_229_fundamental_analysis.f183_191_growth_capability import f183_191_growth_capability_dict
from gulf.tdx.finance.f159_229_fundamental_analysis.f193_209_profitability import f193_209_profitability_dict
from gulf.tdx.finance.f159_229_fundamental_analysis.f210_218_capital_structure import f210_218_capital_structure_dict
from gulf.tdx.finance.f159_229_fundamental_analysis.f219_229_cash_capability import f219_229_cash_capability_dict

f159_229_fundamental_analysis_dict = {
    # 5. 偿债能力分析
    **f159_171_solvency_dict,
    # 6. 经营效率分析
    **f172_182_operating_capability_dict,
    # 7. 发展能力分析
    **f183_191_growth_capability_dict,
    # 8. 获利能力分析
    **f193_209_profitability_dict,
    # 9. 资本结构分析
    **f210_218_capital_structure_dict,
    # 10. 现金流量分析
    **f219_229_cash_capability_dict
}

if __name__ == '__main__':
    pass
