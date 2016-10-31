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

    # return dates, opens, highs, lows, closes, volumes, open_interests
    return data


def main():
    instruments = ["CHRIS/CME_GC1", "CHRIS/CME_CL1"]

    for row in instruments:
        try:
            data = get_data(row)

            sum_close = 0
            count = 0

            for row2 in data:
                try:
                    print(row2)
                    sum_close += row2[4][1]
                    count += 1
                except Exception as e:
                    print("Error at main " + row2)
                    pass

            average_close = sum_close/count
            print(average_close)
        except Exception as e:
            print("Error at main " + row)
            pass


main()
