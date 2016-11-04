from globals import *
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.ticker as tick


dates = []
closes = []

sql_connection = psycopg2.connect(host=host, port=port, user=user, database=database)
cursor = sql_connection.cursor()
cursor.execute("SELECT timestamp, close FROM foreximport WHERE symbol = 'usdjpy' AND date >= '2016-10-29';")
data = cursor.fetchall()
sql_connection.commit()
sql_connection.close()

for row in data:
    dates.append(row[0])
    closes.append(row[1])

fig = plt.figure()
ax1 = plt.subplot(1, 1, 1)
ax1.plot(dates, closes)
for label in ax1.xaxis.get_ticklabels():
    label.set_rotation(45)
plt.show()
