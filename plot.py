import matplotlib.pyplot as plt
import quandl
from matplotlib.finance import candlestick2_ochl

print("Retrieving data ...")

data = quandl.get("CHRIS/CME_GC1", start_date="2016-01-01", end_date="2016-10-07")
data = data.to_dict(orient='index')

series = []
dates = []
opens = []
highs = []
lows = []
closes = []
volumes = []
open_interests = []

for row in data:
    try:
        series.append((row, data[row]['Open'], data[row]['High'], data[row]['Low'], data[row]['Last']
                       , data[row]['Volume'], data[row]['Open Interest']))
    except Exception as e:
        print("error at " + row)
        pass

series.sort()

for row in series:
    try:
        dates.append(row[0])
        opens.append(row[1])
        highs.append(row[2])
        lows.append(row[3])
        closes.append(row[4])
        volumes.append(row[5])
        open_interests.append(row[6])
    except Exception as e:
        print("error at " + row)
        pass

print(dates, closes)

# ax = plt.subplots()
# candlestick2_ochl(ax, opens, closes, highs, lows, width=4, colorup='k', colordown='r', alpha=0.75)

plt.plot(dates, closes)
plt.show()
