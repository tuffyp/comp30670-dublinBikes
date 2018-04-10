# Guide below followed to implement the scraper

#https://maxhalford.github.io/blog/bike-stations/

from six.moves.urllib.request import urlopen
import json
import pandas as pd

from datetime import datetime

key = '41875ce3055e106267cf6104eb90bf8def0adfea'
base = 'https://api.jcdecaux.com/vls/v1/'

def query_API(url):
    # Send a query to the API and decode the bytes it returns
    query = urlopen(url).read().decode('utf-8')
    # Return the obtained string as a dictionary
    return json.loads(query)

def stations_list(city):
    url = base + 'stations/?contract={0}&apiKey={1}'.format(city, key)
    data = query_API(url)
    return data

def timestamp_to_ISO(timestamp):
    moment = datetime.fromtimestamp(timestamp / 1000)
    return moment.time().isoformat()

def information(city):
    # Collect JSON data
    data = stations_list(city)
    # Convert it to a dataframe
    df = pd.io.json.read_json(data)
    # The positions are embedded so they have to be extracted
    positions = df.position.apply(pd.Series)
    df['latitude'] = positions['lat']
    df['longitude'] = positions['lng']
    # Make the timestamps human readable
    df['last_update'] = df['last_update'].apply(timestamp_to_ISO)
    return df[['available_bikes', 'last_update', 'name', 'latitude',
               'longitude', 'available_bike_stands', 'bike_stands',
               'status']]
