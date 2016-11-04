from globals import *
import psycopg2
import pandas as pd
import numpy as np
import talib as ta


sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
sql = "SELECT timestamp, close FROM foreximport WHERE symbol = 'usdjpy';"
data_frame = pd.read_sql(sql, sql_connection)

analysis = pd.DataFrame
data_frame['ema'] = ta.EMA(np.array(data_frame.close), timeperiod=100)
data_frame['rsi'] = ta.RSI(np.array(data_frame.close), timeperiod=10)
data_frame['slow_rsi'] = ta.EMA(np.array(data_frame.rsi), timeperiod=10)
data_frame['roc'] = ta.ROC(np.array(data_frame.close), timeperiod=10)
data_frame['slow_roc'] = ta.EMA(np.array(data_frame.roc), timeperiod=10)
print(data_frame)
