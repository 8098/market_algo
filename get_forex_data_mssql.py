from globals import *
import pymssql
import os
import datetime
import csv


def truncate_db():
    sql_connection = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database=ms_database)
    cursor = sql_connection.cursor()
    cursor.execute("TRUNCATE TABLE Import_QC;")
    cursor.execute("DBCC CHECKIDENT ('[Import_QC]', RESEED, 0);")
    sql_connection.commit()
    sql_connection.close()
    print("Truncated table Import_QC")
    
    
def insert_db(directory):
    for root, dirs, filenames in os.walk(directory):
        for f in filenames:
            symbol = f[:len(f)-4]
            full_filename = directory + '/' + f
            old_filename = directory + '/old_' + f
            
            print("Removing unused columns from data file")
            os.rename(full_filename, old_filename)
            with open(old_filename,"r") as source:
                rdr= csv.reader(source)
                with open(full_filename,"w") as result:
                    wtr= csv.writer(result)
                    in_iter= ((r[0], r[1], r[2], r[3], r[4]) for r in rdr)
                    wtr.writerows(in_iter)
            os.remove(old_filename)

            sql_connection = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database=ms_database)
            cursor = sql_connection.cursor()
            cursor.execute("TRUNCATE TABLE Import_QC_Temp;")
            sql_connection.commit()
            sql_connection.close()
            print("Truncated table Import_QC_Temp")

            print("Inserting data for: " + symbol)
            sql_connection = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database=ms_database)
            cursor = sql_connection.cursor()
            
            # cursor.execute("""BULK INSERT Import_QC_Temp FROM '/root/algo1/test_data/audusd.csv'
            #     WITH (FIELDTERMINATOR=',', ROWTERMINATOR='\n');""")
            # sql_connection.commit()
            # sql_connection.close()
            
            with open(full_filename, 'r') as csv_file:
                reader = csv.reader(csv_file)
                for row in reader:
                    cursor.execute("INSERT INTO Import_QC_Temp ([Timestamp], [Open], [High], [Low], [Close]) "
                                   "VALUES (%s, %d, %d, %d, %d)", (row[0], row[1], row[2], row[3], row[4]))
                    sql_connection.commit()
            csv_file.close()
    
            cursor.execute("""UPDATE Import_QC_Temp SET [symbol] = '%s';""" %symbol)
            sql_connection.commit()
            cursor.close()
            sql_connection.close()
            
            print("Inserting data to Import_QC")
            sql_connection = pymssql.connect(server=ms_server, user=ms_user, password=ms_password, database=ms_database)
            cursor = sql_connection.cursor()
            cursor.execute(
                "INSERT INTO Import_QC ([symbol], [timestamp], [date], [time], [open], [high], [low], [close], [import])"
                "SELECT [symbol], [timestamp], CONVERT(DATE, [timestamp]), CONVERT(TIME, [timestamp])"
                ", [open], [high], [low], [close], GETDATE() FROM Import_QC_Temp;")
            sql_connection.commit()
            cursor.close()
            sql_connection.close()


def main():
    start_time = datetime.datetime.now()
    print("Started at " + str(start_time))
    
    truncate_db()
    insert_db(data_directory)
    
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    print("Finished in " + str(duration) + " minutes")


main()