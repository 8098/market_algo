from globals import *
import quandl
import psycopg2
import matplotlib.pyplot as plt


def get_data(symbol):
    print("Retrieving data for " + symbol + "...")

    quandl.ApiConfig.api_key = quandl_api_key
    quandl_data = quandl.get(symbol, start_date=quandl_start_date, end_date=quandl_end_date)
    quandl_data = quandl_data.to_dict(orient='index')

    series = []
    data = []

    for row in quandl_data:
        try:
            series.append([row
              , quandl_data[row]['Open'] if "nan" not in str(quandl_data[row]['Open']) else 0
              , quandl_data[row]['High'] if "nan" not in str(quandl_data[row]['High']) else 0
              , quandl_data[row]['Low'] if "nan" not in str(quandl_data[row]['Low']) else 0
              , quandl_data[row]['Last'] if "nan" not in str(quandl_data[row]['Last']) else 0
              , quandl_data[row]['Open'] if "nan" not in str(quandl_data[row]['Open']) else 0
              , int(quandl_data[row]['Volume']) if "nan" not in str(quandl_data[row]['Volume']) else 0
              , int(quandl_data[row]['Open Interest']) if "nan" not in str(quandl_data[row]['Open Interest']) else 0])
        except Exception as e:
            print("Error at get_data " + row)

    series.sort()

    for row in series:
        try:
            data.append(row)
        except Exception as e:
            print("Error at get_data " + row)

    return data


def db_insert(symbol, data):
    print("Inserting to database for " + symbol + "...")

    sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
    cursor = sql_connection.cursor()

    count = 0
    for row in data:
        # print(symbol, row[0], row[1], row[2], row[3], row[4], row[5], row[6])
        cursor.execute("""INSERT INTO dataimport (symbol, timestamp, open, high, low, close, volume, openinterest) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
                       , (symbol, row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        count += 1

    sql_connection.commit()
    sql_connection.close()
    print("Finished inserting " + str(count) + " rows to database...")


def db_truncate():
    print("Truncating dataimport table...")

    sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
    cursor = sql_connection.cursor()
    cursor.execute("""TRUNCATE TABLE dataimport;""")
    sql_connection.commit()
    sql_connection.close()
    print("Finished truncating dataimport table...")


def main():
    db_truncate()

    for row in quandl_symbols:
        data = get_data(row)
        db_insert(row, data)


main()
