from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, Integer, Date, Numeric, ForeignKey, MetaData

class SqlApi:
    def __init__(self):
        self.engine = create_engine("mysql://fydata:money@localhost/fypy")

    def init_db(self):
        meta = MetaData()
        
        # Create the tickers table
        tickers = Table(
            'tickers', meta,
            Column('ticker_key', Integer, primary_key = True),
            Column('ticker', String(20))
        )

        # Create the historic data table
        historic = Table(
            'historic', meta,
            Column('ticker_key', Integer, ForeignKey('tickers.ticker_key')),
            Column('date', Date),
            Column('open', Numeric),
            Column('high', Numeric),
            Column('low', Numeric),
            Column('close', Numeric),
            Column('adj_close', Numeric),
            Column('volume', Numeric)
        )

        meta.create_all(self.engine)


    