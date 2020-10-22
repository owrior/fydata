from sqlalchemy import create_engine 
from sqlalchemy import Table, Column, String, Integer, Date, Numeric, ForeignKey, MetaData
from fydata.yahoo_data import dl_yahoo

class SqlApi:
    def __init__(self):
        self.engine = create_engine("mysql://fydata:money@localhost/fypy")

    def df_to_sql(self, df, table, key=None):
        if key is None:
            sql_statement = "INSERT INTO\n\t" + table + " (" + \
            ",".join(df.columns) + ")\nVALUES\n"
        else:
            sql_statement = "INSERT INTO\n\t" + table + " (" \
            + key + "," + ",".join(df.columns) + ")\nVALUES\n"

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

    def new_ticker(self, ticker):
        res = self.engine.execute("SELECT COUNT(*) AS num FROM tickers WHERE ticker = '" + ticker + "';").fetchall()
        if res[0][0] > 0:
            print("Ticker already in database.")
            return 0

        historic = dl_yahoo(ticker)        
        historic.to_csv("~/Data/historic.csv")
        
        self.engine.execute("INSERT INTO tickers ('ticker') VALUES ('" + ticker + "');")


    