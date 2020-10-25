from sqlalchemy import create_engine 
from sqlalchemy import Table, Column, String, Integer, \
                        Date, Numeric, ForeignKey, MetaData
from fydata.yahoo_data import dl_yahoo

class SqlApi:
    """
    """
    def __init__(self):
        self.engine = create_engine("mysql://fydata:money@localhost"\
            "/fypy")

    def df_to_sql(self, df, table):
        sql_statement = "INSERT INTO\n\t" + table + " (" + \
        ",".join(df.columns) + ")\nVALUES\n"

        last = df.index[-1]
        for index, row in df.iterrows():
            if index == last:
                sql_statement += "\t(" + \
                    ",".join([ "'"+str(item)+"'" for item in row]) \
                    + ");\n"
            else:
                sql_statement += "\t(" + \
                    ",".join([ "'"+str(item)+"'" for item in row]) \
                    + "),\n"

        return(sql_statement)

        return 0


    def init_db(self):
        meta = MetaData()
        
        # Create the historic data table
        if self.engine.dialect.has_table(self.engine, "historic"):
            self.engine.execute("DROP TABLE historic;")
        historic = Table(
            'historic', meta,
            Column('ticker_key', Integer, 
                ForeignKey('tickers.ticker_key')),
            Column('date', Date),
            Column('open', Numeric),
            Column('high', Numeric),
            Column('low', Numeric),
            Column('close', Numeric),
            Column('adj_close', Numeric),
            Column('volume', Numeric)
        )
        
        # Create the tickers table
        if self.engine.dialect.has_table(self.engine, "tickers"):
            self.engine.execute("DROP TABLE tickers;")
        tickers = Table(
            'tickers', meta,
            Column('ticker_key', Integer, primary_key = True),
            Column('ticker', String(20))
        )

        meta.create_all(self.engine)

        return 0

    def new_ticker(self, ticker):
        """
        Downloads and adds data for a ticker not within db
        currently.
        :param ticker: The ticker denoting the stock for yahoo.
        :return: zero
        """
        res = self.engine.execute("SELECT COUNT(*) AS num FROM " \
            "tickers WHERE ticker = '"+ticker+"';").fetchone()
        if res is not None:
            print("Ticker already in database.")
            return 0
        
        self.engine.execute("INSERT INTO tickers (ticker) "\
             "VALUES ('" + ticker + "');")
            
        ticker_key = self.engine.execute("SELECT ticker_key FROM "\
            "tickers WHERE ticker = '"+ticker+"';").fetchone()[0]

        historic = dl_yahoo(ticker)
        historic.insert(0, "ticker_key", ticker_key)

        self.engine.execute(self.df_to_sql(historic, "historic"))

        print("Added", ticker)

        return 0

    def update_historic(self, ticker):
        res = self.engine.execute("SELECT a.date " \
            "FROM historic AS a " \
            "INNER JOIN tickers AS b " \
            "ON a.ticker_key = b.ticker_key" \
            "WHERE b.ticker = '"+ticker+"' " \
            "AND a.date = MAX(a.date);").fetchone()
        if res is None:
            print("Ticker is not currently within database. Fetching fresh.")
            self.new_ticker(ticker)
            return 0

        last_date = res[0]

        historic = dl_yahoo(ticker, start_date=last_date)

        self.engine.execute(self.df_to_sql(historic, "historic"))

        return 0


        


    