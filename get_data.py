from globals import *
import quandl
import pymssql
import matplotlib.pyplot as plt


def get_data(symbol):
    print("Retrieving data for " + symbol + "...")

    quandl.ApiConfig.api_key = quandl_api_key
    quandl_data = quandl.get(symbol, start_date="2016-10-03", end_date="2016-10-07")
    quandl_data = quandl_data.to_dict(orient='index')

    series = []
    data = []

    for row in quandl_data:
        try:
            # series.append([('date', row)
            #                   , ('open', quandl_data[row]['Open'])
            #                   , ('high', quandl_data[row]['High'])
            #                   , ('low', quandl_data[row]['Low'])
            #                   , ('close', quandl_data[row]['Last'])
            #                   , ('volume', int(quandl_data[row]['Volume']))
            #                   , ('open_interest', int(quandl_data[row]['Open Interest']))])
            series.append([row, quandl_data[row]['Open']
                              , quandl_data[row]['High']
                              , quandl_data[row]['Low']
                              , quandl_data[row]['Last']
                              , int(quandl_data[row]['Volume'])
                              , int(quandl_data[row]['Open Interest'])])
        except Exception as e:
            print("Error at get_data " + row)
            pass

    series.sort()

    for row in series:
        try:
            data.append(row)
        except Exception as e:
            print("Error at get_data " + row)
            pass

    return data


def db_insert(symbol, data):
    print("Inserting to database...")

    sql_connection = pymssql.connect(server=server, user=user, password=password, database=database)
    print("test")
    cursor = sql_connection.cursor()

    for row in data:
        try:
            print(row)
            cursor.execute("INSERT INTO DataImport VALUES (%s, %s, %d, %d, %d, %d, %d, %d)"
                           , symbol, row[0], row[1], row[2], row[3], row[4], row[5])
        except Exception as e:
            print("Error at db_insert " + row)
            pass

    sql_connection.commit()
    sql_connection.close()

    print("Finished inserting to database...")

def main():
    db_insert("test", "test")
    # for row in quandl_symbols:
    #     try:
    #         data = get_data(row)
    #
    #         sum_close = 0
    #         sum_volume = 0
    #         sum_open_interest = 0
    #         candle_height = 0
    #         count = 0
    #
    #         for row2 in data:
    #             try:
    #                 print(row2)
    #                 sum_close += row2[4]
    #                 sum_volume += row2[5]
    #                 sum_open_interest += row2[6]
    #                 candle_height += (row2[2] - row2[3])/row2[4]
    #                 count += 1
    #             except Exception as e:
    #                 print("Error at main " + row2)
    #                 pass
    #
    #         average_close = sum_close/count
    #         average_volume = sum_volume / count
    #         average_open_interest = sum_open_interest / count
    #         average_candle_height = candle_height/count*100
    #         print("Average Close: ", average_close)
    #         print("Average Volume: ", average_volume)
    #         print("Average Open Interest: ", average_open_interest)
    #         print("Average Candle Height: ", average_candle_height)
    #
    #         db_insert(row, data)
    #     except Exception as e:
    #         print("Error at main " + row)
    #         pass


main()
