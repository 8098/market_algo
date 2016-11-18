from globals import *
import psycopg2
import os
import datetime
import csv


def truncate_db():
    sql_connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = sql_connection.cursor()
    cursor.execute("TRUNCATE TABLE import_qc_hour RESTART IDENTITY;")
    sql_connection.commit()
    sql_connection.close()
    print("Truncated table import_qc_hour")


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

            sql_connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
            cursor = sql_connection.cursor()
            cursor.execute("TRUNCATE TABLE import_qc_hour_temp;")
            sql_connection.commit()
            sql_connection.close()
            print("Truncated table import_qc_hour_temp")

            print("Inserting data for: " + symbol)
            sql_connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
            cursor = sql_connection.cursor()
            data_csv = open(full_filename, 'r')
            cursor.copy_from(data_csv, 'import_qc_hour_temp (p_timestamp, p_open, p_high, p_low, p_close)', sep=',')
            data_csv.close()
            sql_connection.commit()

            cursor.execute("""UPDATE import_qc_hour_temp SET symbol = '%s';""" %symbol)
            sql_connection.commit()
            sql_connection.close()

            print("Inserting data to import_qc_hour")
            sql_connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
            cursor = sql_connection.cursor()
            cursor.execute(
                "INSERT INTO import_qc_hour (symbol, p_timestamp, p_date, p_time, p_open, p_high, p_low, p_close, import)"
                "SELECT symbol, p_timestamp, p_timestamp::date, p_timestamp::time"
                ", p_open, p_high, p_low, p_close, NOW() FROM import_qc_hour_temp;")
            sql_connection.commit()
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
