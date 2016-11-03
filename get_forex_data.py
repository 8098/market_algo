from globals import *
import psycopg2
import os

symbol = 'AUDUSD'
directory = 'C:/Users/jcampana/Desktop/' + symbol

sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
cursor = sql_connection.cursor()
cursor.execute("""TRUNCATE TABLE foreximporttemp;""")
sql_connection.commit()
sql_connection.close()

print("Truncated table foreximporttemp")

for root, dirs, filenames in os.walk(directory):
    for f in filenames:
        date = f[:8]
        full_filename = directory + '/' + f

        print("Inserting data for: " + symbol + " --- " + date)

        sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
        cursor = sql_connection.cursor()

        csv = open(full_filename, 'r')
        cursor.copy_from(csv, 'foreximporttemp (minute, open, high, low, close)', sep=',')
        csv.close()

        cursor.execute("""UPDATE foreximporttemp SET symbol = %s, date = %s;""", (symbol, date))

        sql_connection.commit()
        sql_connection.close()
