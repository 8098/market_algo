from globals import *
import psycopg2
import pandas as pd


sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
sql = "SELECT timestamp, close FROM foreximport WHERE symbol = 'usdjpy';"
data_frame = pd.read_sql(sql, sql_connection)
print(data_frame)
