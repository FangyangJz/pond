import threading
import time
from datetime import date

import akshare as ak
import pandas as pd
from tqdm import tqdm

from gulf.akshare.const import AKSHARE_CRAWL_CONCURRENT_LIMIT, AKSHARE_CRAWL_STOP_INTERVEL
from gulf.akshare.stock.index_decorator import trans_ch_col_name


def update_index_concept_em_df_thread(symbol, start_date, end_date, res_dict):
    if start_date is not None:
        # 数据带复权, 可以指定起始结束日期
        df = ak.stock_board_concept_hist_em(
            symbol=symbol,
            start_date=start_date,
            end_date=date.today().strftime("%Y%m%d") if end_date is None else end_date,
            period="daily",
            adjust="hfq"
        )
    else:
        # 返回从开始到现在的全部数据, 未复权, 复权对于行业指数有用??
        df = ak.stock_board_concept_hist_min_em(symbol=symbol, period="101")

    df['板块名称'] = symbol
    res_dict[symbol] = df


@trans_ch_col_name
def get_stock_index_concept_em_daily_df(start_date=None, end_date=None) -> pd.DataFrame:
    ind_name = ak.stock_board_concept_name_em()
    t_list = []
    res_dict = dict()
    for bk_name in tqdm(ind_name['板块名称'].values):
        # update_index_concept_em_df_thread(bk_name, start_date, end_date, res_dict)
        t = threading.Thread(
            target=update_index_concept_em_df_thread,
            args=(bk_name, start_date, end_date, res_dict)
        )
        t.start()
        t_list.append(t)

        if len(t_list) > AKSHARE_CRAWL_CONCURRENT_LIMIT:
            [t.join() for t in t_list]
            t_list = []

            print(f" In case of ban ip, wait {AKSHARE_CRAWL_STOP_INTERVEL}s ...")
            time.sleep(AKSHARE_CRAWL_STOP_INTERVEL)

    [t.join() for t in t_list]
    return pd.concat(list(res_dict.values()))


if __name__ == '__main__':
    file_name = "stock_index_concept_em_daily_df.pkl"
    # stock_index_concept_em_daily_df = get_stock_index_concept_em_daily_df(start_date="20230101")
    # stock_index_concept_em_daily_df = get_stock_index_concept_em_daily_df(start_date="20230113", end_date="20230113")
    stock_index_concept_em_daily_df = get_stock_index_concept_em_daily_df()
    # stock_index_concept_em_daily_df.to_pickle(file_name)
    stock_index_concept_em_daily_df = pd.read_pickle(file_name)
    print(1)

    # ind_name = ak.stock_board_industry_name_em()
    # ths_ind_name1 = ak.stock_board_industry_name_ths()
    # ths_ind_name2 = ak.stock_board_industry_summary_ths()
    # print(1)

    # stock_board_change_em_df = ak.stock_board_change_em()
    # print(stock_board_change_em_df)

    # cc = ak.stock_board_concept_name_ths()
    #
    # stock_board_concept_hist_ths_df = ak.stock_board_concept_hist_ths(start_year='2020', symbol="超级品牌")
    # print(stock_board_concept_hist_ths_df)
