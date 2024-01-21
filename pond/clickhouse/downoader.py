import ray
from datetime import datetime
import akshare as ak

@ray.remote
class Downloader:
    
    def __init__(self) -> None:
        self.df = None

    def get(self):
        return self.df

    def download(self, func, **kwargs):
        if func is None:
            return
        self.df = func(**kwargs)