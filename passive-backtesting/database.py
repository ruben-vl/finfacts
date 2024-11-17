import sqlite3

class StockDataDB:

    def __init__(self, verbose=False):
        self.connection = sqlite3.connect('finfacts.db')
        self.cursor = self.connection.cursor()
        self._create_meta_table_if_not_exists()
        self._create_absolutes_table_if_not_exists()
        self._create_ratios_table_if_not_exists()
        self._create_events_table_if_not_exists()
        self.verbose = verbose

    def _create_meta_table_if_not_exists(self):
        create_metadata_table = '''
            CREATE TABLE IF NOT EXISTS META (
                isin TEXT NOT NULL PRIMARY KEY, 
                symbol TEXT NOT NULL, 
                name TEXT NOT NULL,
                currency TEXT NOT NULL,
                exchange TEXT NOT NULL,
                firstTradeDate TEXT NOT NULL
            );'''
        self.cursor.execute(create_metadata_table)
        self.connection.commit()

    def add_metadata(self, isin: str, symbol: str, name: str, currency: str,
                     exchange: str, first_trade_date: str):

        insert_metadata = f'''
            INSERT INTO META
            (isin, symbol, name, currency, exchange, firstTradeDate)
            VALUES ('{isin}', '{symbol}', '{name}', '{currency}', '{exchange}', 
            '{first_trade_date}')'''
        try:
            self.cursor.execute(insert_metadata)
            self.connection.commit()
        except sqlite3.IntegrityError as _:
            if self.verbose:
                print(f"-db- Metadata for {isin} already exists in the database")
        except sqlite3.Error as err:
            print(err)

    def _create_absolutes_table_if_not_exists(self):
        create_absolutes_table = '''
            CREATE TABLE IF NOT EXISTS ABSOLUTES (
                isin TEXT NOT NULL,
                date TEXT NOT NULL,
                marketCap INTEGER,
                enterpriseValue INTEGER,
                averageVolume INTEGER,
                averageVolume10days INTEGER,
                PRIMARY KEY (isin, date)
            );'''
        self.cursor.execute(create_absolutes_table)
        self.connection.commit()

    def add_absolutes(self, isin: str, date: str, market_cap: int|None,
                      enterprise_value: int|None, average_volume: int|None,
                      average_volume_10days: int|None):
        insert_absolutes = f'''
            INSERT INTO ABSOLUTES
            (isin, date, marketCap, enterpriseValue, averageVolume, 
            averageVolume10days)
            VALUES ('{isin}', '{date}', 
            '{"NULL" if market_cap is None else market_cap}', 
            '{"NULL" if enterprise_value is None else enterprise_value}', 
            '{"NULL" if average_volume is None else average_volume}', 
            '{"NULL" if average_volume_10days is None else average_volume_10days}')'''
        try:
            self.cursor.execute(insert_absolutes)
            self.connection.commit()
        except sqlite3.IntegrityError as _:
            if self.verbose:
                print(f"-db- Absolutes data for {isin} on {date} already exists in the database")
        except sqlite3.Error as err:
            print(err)

    def _create_ratios_table_if_not_exists(self):
        create_ratios_table = '''
            CREATE TABLE IF NOT EXISTS RATIOS (
                isin TEXT NOT NULL,
                date TEXT NOT NULL,
                beta REAL,
                trailingPE REAL,
                forwardPE REAL,
                priceToBook REAL,
                trailingEps REAL,
                forwardEps REAL,
                enterpriseToRevenue REAL,
                enterpriseToEbitda REAL,
                PRIMARY KEY (isin, date)
            );'''
        self.cursor.execute(create_ratios_table)
        self.connection.commit()

    def add_ratios(self, isin: str, date: str, beta: float|None, trailing_pe: float|None,
                   forward_pe: float|None, price_to_book: float|None, trailing_eps: float|None,
                   forward_eps: float|None, enterprise_to_revenue: float|None,
                   enterprise_to_ebitda: float|None):
        insert_ratios = f'''
            INSERT INTO RATIOS
            (isin, date, beta, trailingPE, forwardPE, priceToBook, trailingEps,
            forwardEps, enterpriseToRevenue, enterpriseToEbitda)
            VALUES ('{isin}', '{date}', '{"NULL" if beta is None else beta}', 
            '{"NULL" if trailing_pe is None else trailing_pe}', 
            '{"NULL" if forward_pe is None else forward_pe}', 
            '{"NULL" if price_to_book is None else price_to_book}', 
            '{"NULL" if trailing_eps is None else trailing_eps}', 
            '{"NULL" if forward_eps is None else forward_eps}', 
            '{"NULL" if enterprise_to_revenue is None else enterprise_to_revenue}',
            '{"NULL" if enterprise_to_ebitda is None else enterprise_to_ebitda}')'''
        try:
            self.cursor.execute(insert_ratios)
            self.connection.commit()
        except sqlite3.IntegrityError as _:
            if self.verbose:
                print(f"-db- Ratios data for {isin} on {date} already exists in the database")
        except sqlite3.Error as err:
            print(err)

    def _create_events_table_if_not_exists(self):
        create_events_table = '''
            CREATE TABLE IF NOT EXISTS EVENTS (
                isin TEXT NOT NULL,
                date TEXT NOT NULL,
                eventType TEXT NOT NULL,
                PRIMARY KEY (isin, date, eventType)
            );'''
        self.cursor.execute(create_events_table)
        self.connection.commit()

    def add_event(self, isin: str, date: str, event_type: str):
        insert_events = f'''
            INSERT INTO EVENTS
            (isin, date, eventType)
            VALUES ('{isin}', '{date}', '{event_type}')'''
        try:
            self.cursor.execute(insert_events)
            self.connection.commit()
        except sqlite3.IntegrityError as _:
            if self.verbose:
                print(f"-db- {event_type} events data for {isin} on {date} already exists in the database")
        except sqlite3.Error as err:
            print(err)
