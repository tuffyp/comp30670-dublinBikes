from lib import JCDecaux as jcd
import time

city = "Dublin"

#times = time.strftime("%Y%m%d-%H%M%S")

#citytimes= city + times

while True:
    dataframe = jcd.information(city)
    dataframe.to_csv('static/data/{0}.csv'.format(city), index=False)
    time.sleep(300)

