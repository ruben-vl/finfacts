import os
import json

import yfinance as yf

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


def save_monthly_candles(isin: str):
    data_cache_path = "/home/ruben/Projects/finfacts/cache/"
    write_path = data_cache_path + f"{isin}-p_max-int_1mo.csv"
    print(f"Saving monthly stock data for {isin}")
    if not os.path.exists(write_path):
        try:
            ticker = yf.Ticker(isin, session=session)
            monthly_history = ticker.history(period="max", interval="1mo")
            monthly_history.to_csv(write_path, sep=',')
            print(f"Stock data saved for {isin}")
        except Exception as e:
            print(f"Exception occurred for {isin}: {e}")
    else:
        print(f"Data for {isin} already exists")

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(1, Duration.SECOND*5)),
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache")
)

with open("/passive-backtesting/beursrally/assets_filtered.json", "r") as file:
    data = json.load(file)
    
stock_isins = data["Stock"].keys()

for isin in stock_isins:
    save_monthly_candles(isin)
