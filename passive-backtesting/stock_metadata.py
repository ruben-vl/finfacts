import yfinance as yf

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

from datetime import datetime
from database import StockDataDB


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


class StockMetadata:

    def __init__(self):
        self.session = CachedLimiterSession(
            limiter=Limiter(RequestRate(1, Duration.SECOND * 5)),
            bucket_class=MemoryQueueBucket,
            backend=SQLiteCache("yfinance.cache")
        )

    def store_metadata(self, isin: str):
        print(f"Storing metadata for {isin}")
        date_string = datetime.today().strftime("%d_%m_%Y")
        ticker = yf.Ticker(isin, session=self.session)
        database = StockDataDB()

        info = ticker.info
        calendar = ticker.calendar

        database.add_metadata(isin=isin,
                              symbol=info['symbol'],
                              name=info['shortName'],
                              currency=info['currency'],
                              exchange=info['exchange'],
                              first_trade_date=datetime.fromtimestamp(
                                  info['firstTradeDateEpochUtc']).strftime("%d_%m_%Y"))

        database.add_absolutes(isin=isin,
                               date=date_string,
                               market_cap=info['marketCap'],
                               enterprise_value=info['enterpriseValue'],
                               average_volume=info['averageVolume'],
                               average_volume_10days=info['averageVolume10days'])

        database.add_ratios(isin=isin,
                            date=date_string,
                            beta=info['beta'],
                            trailing_pe=info['trailingPE'],
                            forward_pe=info['forwardPE'],
                            price_to_book=info['priceToBook'],
                            trailing_eps=info['trailingEps'],
                            forward_eps=info['forwardEps'],
                            enterprise_to_revenue=info['enterpriseToRevenue'],
                            enterprise_to_ebitda=info['enterpriseToEbitda'])

        ex_dividend_date = calendar['Ex-Dividend Date']
        database.add_event(isin=isin, date=ex_dividend_date.strftime("%d_%m_%Y"), event_type="Ex-Dividend")
        dividend_date = calendar['Dividend Date']
        database.add_event(isin=isin, date=dividend_date.strftime("%d_%m_%Y"), event_type="Dividend")

        earnings = calendar['Earnings Date']
        for date in earnings:
            database.add_event(isin=isin, date=date.strftime("%d_%m_%Y"), event_type="Earnings")

        for key in calendar.keys():
            if key not in {'Dividend Date', 'Ex-Dividend Date', 'Earnings Date', 'Earnings High', 'Earnings Low',
                           'Earnings Average', 'Revenue High', 'Revenue Low', 'Revenue Average'}:
                print(f"Unknown key in calendar: {key}")

    @classmethod
    def _filtered_info(cls, ticker: yf.ticker.Ticker) -> dict:
        info = ticker.info
        calendar = ticker.calendar
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
            },
            'events': {
                'ex-dividend': calendar['Ex-Dividend Date'],
                'earnings': calendar['Earnings Date']
            }
        }

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
