import io
import datetime
import requests
import pandas as pd
from fydata.url_builder import yahoo_url


class DataManager:
    def __init__(self, ticker, start_date=datetime.date(2010, 1, 1), end_date=datetime.date.today(), period=None, frequency="1d"):
        self.url = yahoo_url(ticker, start_date, end_date, period, frequency)

    def dl_data(self):
        r = requests.get(self.url)
        if r.ok:
            data = r.content.decode("utf-8")
            self.df = pd.read_csv(io.StringIO(data))
