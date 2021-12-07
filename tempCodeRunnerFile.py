from __future__ import print_function
import time
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd


import gspread_pandas as gsp

secret = gsp.conf.get_config(conf_dir='.', file_name='google_secret.json')
client = gsp.Client(user='cd002009@gmail.com', config=secret)
# print(secret)