# !/usr/bin/env python3
# @Datetime : 2023/11/27 上午 12:25
# @Author   : Fangyang
# @Software : PyCharm

import pandas as pd
import polars as pl
import pyarrow as pa
from typing import Literal
from dataclasses import dataclass

DataFrameStrType = Literal["pandas", "polars", "arrow"]
DataFrameType = pd.DataFrame | pl.DataFrame | pa.lib.Table


@dataclass
class DFType:
    pandas: DataFrameStrType = "pandas"
    polars: DataFrameStrType = "polars"
    arrow: DataFrameStrType = "arrow"


df_types = DFType()

if __name__ == "__main__":
    xx = df_types.polars
    print(1)
