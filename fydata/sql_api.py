import io
import datetime
import requests
import pandas as pd
from fydata.url_builder import yahoo_url


class SqlApi:
    def __init__(self):
        self.db = "mysql"