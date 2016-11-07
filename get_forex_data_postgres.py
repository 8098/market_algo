from globals import *
import psycopg2
import os


def truncate_db():
    sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
    cursor = sql_connection.cursor()
    cursor.execute("TRUNCATE TABLE foreximport RESTART IDENTITY;")
    sql_connection.commit()
    sql_connection.close()
    print("Truncated table foreximport")


def insert_db(directory):
    for root, dirs, filenames in os.walk(directory):
        for f in filenames:
            symbol = f[:6]
            full_filename = directory + '/' + f

            sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
            cursor = sql_connection.cursor()
            cursor.execute("TRUNCATE TABLE foreximporttemp;")
            sql_connection.commit()
            sql_connection.close()
            print("Truncated table foreximporttemp")

            print("Inserting data for: " + symbol)
            sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
            cursor = sql_connection.cursor()
            csv = open(full_filename, 'r')
            cursor.copy_from(csv, 'foreximporttemp (timestamp, open, high, low, close)', sep=',')
            csv.close()
            sql_connection.commit()

            cursor.execute("""UPDATE foreximporttemp SET symbol = '%s';""" %symbol)
            sql_connection.commit()
            sql_connection.close()

            print("Inserting data to foreximport")
            sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
            cursor = sql_connection.cursor()
            cursor.execute(
                "INSERT INTO foreximport (symbol, timestamp, date, time, open, high, low, close, import)"
                "SELECT symbol, timestamp, timestamp::date, timestamp::time"
                ", open, high, low, close, NOW() FROM foreximporttemp WHERE timestamp::date >= '2006-01-01';")
            sql_connection.commit()
            sql_connection.close()


def main():
    truncate_db()
    insert_db(data_directory)


main()
