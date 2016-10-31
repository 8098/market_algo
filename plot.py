import matplotlib.pyplot as plt
import quandl


def get_data(instrument):
    print("Retrieving data for " + instrument)

    quandl.ApiConfig.api_key = 'gzBJon8Nc6pbn6WytL_H'
    quandl_data = quandl.get(instrument, start_date="2016-10-03", end_date="2016-10-07")
    quandl_data = quandl_data.to_dict(orient='index')

    series = []
    data = []

    for row in quandl_data:
        try:
            series.append([('date', row)
                              , ('open', quandl_data[row]['Open'])
                              , ('high', quandl_data[row]['High'])
                              , ('low', quandl_data[row]['Low'])
                              , ('close', quandl_data[row]['Last'])
                              , ('volume', int(quandl_data[row]['Volume']))
                              , ('open_interest', int(quandl_data[row]['Open Interest']))])
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


def main():
    instruments = ["CHRIS/CME_GC1", "CHRIS/CME_CL1"]

    for row in instruments:
        try:
            data = get_data(row)

            sum_close = 0
            sum_volume = 0
            sum_open_interest = 0
            candle_height = 0
            count = 0

            for row2 in data:
                try:
                    print(row2)
                    sum_close += row2[4][1]
                    sum_volume += row2[5][1]
                    sum_open_interest += row2[6][1]
                    candle_height += (row2[2][1] - row2[3][1])/row2[4][1]
                    count += 1
                except Exception as e:
                    print("Error at main " + row2)
                    pass

            average_close = sum_close/count
            average_volume = sum_volume / count
            average_open_interest = sum_open_interest / count
            average_candle_height = candle_height/count*100
            print("Average Close: ", average_close)
            print("Average Volume: ", average_volume)
            print("Average Open Interest: ", average_open_interest)
            print("Average Candle Height: ", average_candle_height)
        except Exception as e:
            print("Error at main " + row)
            pass


main()
