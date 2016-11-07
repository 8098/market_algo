from globals import *
import pymssql
import os
import csv
from datetime import datetime

directory = 'C:\Data'
start_time = datetime.now()
print("Started at " + str(start_time))

sql_connection = pymssql.connect(server=server, user=user, password=password, database=database)
cursor = sql_connection.cursor()
cursor.execute("TRUNCATE TABLE foreximport;")
cursor.execute("DBCC CHECKIDENT ('[foreximport]', RESEED, 1);")
sql_connection.commit()
sql_connection.close()
print("Truncated table foreximport")

for root, dirs, filenames in os.walk(directory):
    for f in filenames:
        symbol = f[:6]
        full_filename = directory + '/' + f

        sql_connection = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = sql_connection.cursor()
        cursor.execute("TRUNCATE TABLE foreximporttemp;")
        sql_connection.commit()
        cursor.close()
        sql_connection.close()
        print("Truncated table foreximporttemp")

        print("Inserting data for: " + symbol)
        sql_connection = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = sql_connection.cursor()

        with open(full_filename, 'r') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                cursor.execute("INSERT INTO foreximporttemp ([timestamp], [open], [high], [low], [close]) "
                               "VALUES (%s, %d, %d, %d, %d)", (row[0], row[1], row[2], row[3], row[4]))
                sql_connection.commit()
        csv_file.close()

        cursor.execute("""UPDATE foreximporttemp SET [symbol] = '%s';""" %symbol)
        sql_connection.commit()
        cursor.close()
        sql_connection.close()

        print("Inserting data to foreximport")
        sql_connection = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = sql_connection.cursor()
        cursor.execute(
            "INSERT INTO foreximport ([symbol], [timestamp], [date], [time], [open], [high], [low], [close], [import])"
            "SELECT [symbol], [timestamp], CONVERT(DATE, [timestamp]), CONVERT(TIME, [timestamp])"
            ", [open], [high], [low], [close], GETDATE() FROM foreximporttemp "
            "WHERE CONVERT(DATE, [timestamp]) >= '2006-01-01';")
        sql_connection.commit()
        cursor.close()
        sql_connection.close()

end_time = datetime.now()
print("Ended at " + str(end_time))
