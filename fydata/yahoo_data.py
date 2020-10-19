import io
import datetime
import requests
import pandas as pd

def yahoo_url(ticker, start_date, end_date, period, frequency):
    """
    Function to create url string for data fetch from yahoo.
    :param ticker:
    :param start_date:
    :param end_date:
    :param period:
    :param frequency: 1d, 1wk, 1mo
    :return:
    """
    date_ref = datetime.date(1970, 1, 1)

    if period is None:
        period1 = int((start_date - date_ref).total_seconds())
    else:
        start_date = end_date - period
        period1 = int((start_date - date_ref).total_seconds())

    period2 = int((end_date - date_ref).total_seconds())
    root = "https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={period1}&period2={period2}&interval={frequency}&events=history"

    built_url = root.format(ticker=ticker, period1=period1, period2=period2, frequency=frequency)

    return built_url

def dl_yahoo(ticker, start_date=datetime.date(2010, 1, 1), end_date=datetime.date.today(), period=None, frequency="1d"):
    r = requests.get(yahoo_url(ticker, start_date, end_date, period, frequency))
    if r.ok:
        # Reading in data as a csv
        data = r.content.decode("utf-8")
        df = pd.read_csv(io.StringIO(data))

        # Renaming for consistency
        df.columns = df.columns.str.lower()
        df = df.rename(columns={"adj close": "adj_close"})
        
        # Format the date column.
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        return df
    else:
        print("Error downloading.")
        return 0
