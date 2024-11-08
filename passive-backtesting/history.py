import yfinance as yf

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

import json

def save_monthly_candles(isin: str):
    ticker = yf.Ticker(isin, session=session)
    monthly_history = ticker.history(period="max", interval="1mo")
    data_cache_path = "/home/ruben/Projects/finfacts/cache/"
    monthly_history.to_csv(data_cache_path + f"{isin}-p_max-int_1mo.csv", sep=',')

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(1, Duration.SECOND*5)),
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache")
)

with open("/home/ruben/Projects/finfacts/beursrally/assets_filtered.json", "r") as file:
    data = json.load(file)
    
stock_isins = data["Stock"].keys()

for isin in stock_isins:
    save_monthly_candles(isin)
