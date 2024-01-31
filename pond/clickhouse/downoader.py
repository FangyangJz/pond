import ray
from datetime import datetime
import akshare as ak
import pandas as pd

@ray.remote
class Downloader:
    
    def __init__(self) -> None:
        self.dfs = []

    def get(self):
        if len(self.dfs) == 0:
            return []
        elif len(self.dfs) == 1:
            return self.dfs[0]
        else:
            return pd.concat(self.dfs)

    def download(self, func, **kwargs):
        if func is None:
            return
        try:
            df = func(**kwargs)
            if df is not None and len(df) > 0:
                self.dfs.append(df)
        except Exception as e:
            print(e)