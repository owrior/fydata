import sys
import datetime as dt
import time
from fydata.yahoo_data import dl_yahoo
from fydata.sql_api import SqlApi

sql = SqlApi()

sql.init_db()

sql.new_ticker("GOOG")