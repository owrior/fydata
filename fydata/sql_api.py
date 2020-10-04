import io
import datetime
import requests
import pandas as pd

class SqlApi:
    def __init__(self):
        self.db = "mysql"