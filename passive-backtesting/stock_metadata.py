import yfinance as yf
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from datetime import datetime


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


class StockMetadata:

    def __init__(self):
        self.session = CachedLimiterSession(
            limiter=Limiter(RequestRate(1, Duration.SECOND*5)),
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache")
        )

    @classmethod
    def _get_ex_dividend_date(cls, ticker: yf.ticker.Ticker):
        return ticker.calendar['Ex-Dividend Date']

    @classmethod
    def _get_earnings_date(cls, ticker: yf.ticker.Ticker):
        earnings_date = ticker.calendar['Earnings Date']
        if len(earnings_date) == 1:
            return earnings_date[0]
        else:
            print(f"More than 1 earnings date for ticker {ticker.isin}")
            return earnings_date

    @classmethod
    def _filtered_info(cls, ticker: yf.ticker.Ticker) -> dict:
        info = ticker.info
        return {
            'meta': {
                'isin': ticker.isin,
                'symbol': info['symbol'],
                'name': info['shortName'],
                'currency': info['currency'],
                'exchange': info['exchange'],
                'firstTradeDate': datetime.fromtimestamp(info['firstTradeDateEpochUtc'])
            },
            'absolutes': {
                'marketCap': info['marketCap'],
                'enterpriseValue': info['enterpriseValue'],
                'averageVolume': info['averageVolume'],
                'averageVolume10days': info['averageVolume10days']
            },
            'ratios': {
                'beta': info['beta'],
                'trailingPE': info['trailingPE'],
                'forwardPE': info['forwardPE'],
                'priceToBook': info['priceToBook'],
                'trailingEps': info['trailingEps'],
                'forwardEps': info['forwardEps'],
                'enterpriseToRevenue': info['enterpriseToRevenue'],
                'enterpriseToEbitda': info['enterpriseToEbitda']
            }
        }


    @classmethod
    def _get_market_cap(cls, ticker: yf.ticker.Ticker):
        return ticker.info['marketCap']

    @classmethod
    def _get_beta(cls, ticker: yf.ticker.Ticker):
        return ticker.info['beta']

    @classmethod
    def _get_trailing_pe(cls, ticker: yf.ticker.Ticker):
        return ticker.info['trailingPE']

    @classmethod
    def _get_forward_pe(cls, ticker: yf.ticker.Ticker):
        return ticker.info['forwardPE']

    @classmethod
    def _get_avg_volume(cls, ticker: yf.ticker.Ticker):
        return ticker.info['averageVolume']

    @classmethod
    def _get_avg_volume_10days(cls, ticker: yf.ticker.Ticker):
        return ticker.info['averageVolume10days']

    @classmethod
    def _get_currency(cls, ticker: yf.ticker.Ticker):
        return ticker.info['currency']

    @classmethod
    def _get_enterprise_value(cls, ticker: yf.ticker.Ticker):
        return ticker.info['enterpriseValue']

    @classmethod
    def _get_price_to_book(cls, ticker: yf.ticker.Ticker):
        return ticker.info['priceToBook']

    @classmethod
    def _get_trailing_eps(cls, ticker: yf.ticker.Ticker):
        return ticker.info['trailingEps']

    @classmethod
    def _get_forward_eps(cls, ticker: yf.ticker.Ticker):
        return ticker.info['forwardEps']

    @classmethod
    def _get_enterprise_to_revenue(cls, ticker: yf.ticker.Ticker):
        return ticker.info['enterpriseToRevenue']

    @classmethod
    def _get_enterprise_to_ebitda(cls, ticker: yf.ticker.Ticker):
        return ticker.info['enterpriseToEbitda']

    @classmethod
    def _get_exchange(cls, ticker: yf.ticker.Ticker):
        return ticker.info['exchange']

    @classmethod
    def _get_symbol(cls, ticker: yf.ticker.Ticker):
        return ticker.info['symbol']

    @classmethod
    def _get_short_name(cls, ticker: yf.ticker.Ticker):
        return ticker.info['shortName']

    @classmethod
    def _get_first_trade_date(cls, ticker: yf.ticker.Ticker):
        return ticker.info['firstTradeDateEpochUtc']


    # def stock_meta(self):
    #     ticker = yf.Ticker("BE0003555639", session=self.session)
    #     info = ticker.info
    #     for item in info.items():
    #         print(item)
    #     print(ticker.calendar)
    #     # print(ticker.news)

    # def get_stock_ages_months(self) -> list[tuple[str, int]]:
    #     stock_isins = BeursrallyAssets.stock_isins()
    #     print(stock_isins)
    #
    #     return []


# if __name__ == "__main__":
#     BeursrallyData().stock_meta()
