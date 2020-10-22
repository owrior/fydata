import sys
import datetime as dt
import time
from fydata.yahoo_data import dl_yahoo
from fydata.sql_api import SqlApi

goog = dl_yahoo("GOOG")
print(goog.info())
print(goog.head())

sql = SqlApi()

df = dl_yahoo("GOOG")

print(sql.df_to_sql(df, "historic", "ticker_key"))