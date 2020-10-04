import sys
import datetime as dt
import time
from fydata.data_manager import DataManager

dm = DataManager("GOOG")
dm.dl_data()

print(dm.df)