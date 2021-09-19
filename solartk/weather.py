import datetime
import time
import pytz
import pandas as pd 
import json
import urllib.request
import requests
from tzwhere import tzwhere
from darksky import forecast 
import numpy as np

from helpers import okta_to_percent, granularity_to_freq

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import requests_cache

session = requests_cache.CachedSession('demo_cache')

def get_temperature_cloudcover(start_time=None, end_time=None, 
                granularity=None,latitude=None, longitude=None, source='weather_underground', timezone='US/Eastern', darksky_api_key=None):

    if (source == 'weather_underground' or darksky_api_key == None):

        # create a pandas datetimeindex 
        df = pd.date_range(start_time - datetime.timedelta(days=1), end_time + datetime.timedelta(days=1) , freq='D')

        # logger.info(f"Date Range: {df}")

        # convert it into a simple dataframe and rename the column
        df = df.to_frame(index=False)
        df.columns = ['time']

        # convert it into required format for weather underground
        df['time'] = df['time'].dt.strftime('%Y%m%d')

        # logger.info(df)

        temp_cloud_df = pd.DataFrame()

        for _ , row in df.iterrows():
            # print(row['time'])
            try:
                url = "https://api.weather.com/v1/geocode/{}/{}/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&startDate={}&endDate={}&units=e".format(latitude, longitude, row['time'], row['time'])
                # data = urllib.request.urlopen(url).read()
                data = session.get(url).text
                output = json.loads(data)
                output= pd.DataFrame(output['observations'])
                output = output[['valid_time_gmt', 'temp', 'clds', 'wx_phrase']]
                output.columns = ['time', 'temperature', 'clds', 'wx_phrase']
                temp_cloud_df = temp_cloud_df.append(output, ignore_index=True)
            except urllib.error.HTTPError as e:
                # print(e)
                pass
            # time.sleep(0.01)

        
        # convert to datetime and set the correct timezone
        temp_cloud_df['time_s'] = temp_cloud_df['time']
        # temp_cloud_df['time'] = pd.to_datetime(temp_cloud_df['time'],unit='s').dt.tz_localize('utc').dt.tz_convert(timezone)
        temp_cloud_df['time'] = pd.to_datetime(temp_cloud_df['time'],unit='s').dt.tz_localize('utc').dt.tz_convert(timezone)
        # temp_cloud_df['time'] = temp_cloud_df['time'].dt.round("H")

        logger.info(f"Weather Obs: {temp_cloud_df}")

        # resample the data to desired granularity
        temp_cloud_df = temp_cloud_df.set_index(temp_cloud_df['time'])
        temp_cloud_df = temp_cloud_df.resample(granularity_to_freq(granularity)).ffill().fillna(method='ffill')
        temp_cloud_df = temp_cloud_df.fillna(method='bfill')
        # logger.info(f"Weather Obs (Interpolated): {temp_cloud_df}")
        temp_cloud_df = temp_cloud_df[['temperature', 'clds']]
        temp_cloud_df = temp_cloud_df.reset_index()

        # chnage to C from F
        temp_cloud_df['temperature'] = (temp_cloud_df['temperature'] - 32) * 5/9

        # cloud okta code to percent
        temp_cloud_df['clouds'] = pd.to_numeric(temp_cloud_df['clds'].apply(lambda x: okta_to_percent(x)))

        # keep only relevant columns
        temp_cloud_df = temp_cloud_df[['time', 'temperature', 'clouds', 'clds']]

        ######################### future release ############################
        # # create a pandas datetimeindex 
        # df = pd.date_range(start_time, end_time, freq=granularity_to_freq(granularity), tz=timezone)

        # # convert it into a simple dataframe and rename the column
        # df = df.to_frame(index=False)
        # df.columns = ['time']

        # # combine both df and temperature_df
        # temp_cloud_df = df.join(temp_cloud_df.set_index('time'), on='time')
        ####################################################################

        # temp_cloud_df['time'] = temp_cloud_df['time'].dt.tz_localize('utc').dt.tz_convert(timezone)
        temp_cloud_df['time'] = temp_cloud_df['time'].dt.tz_localize(None)
        # temp_cloud_df['time'] = temp_cloud_df['time'].dt.tz_convert(timezone)
        

        # print(temp_cloud_df)

    elif (source == 'darksky' and darksky_api_key != None):
        time = []
        temperature = []
        cloudcover = []
        summary = []
        
        # localizing the datetime based on the timezone
        start: datetime.datetime = timezone.localize(start_time)
        end: datetime.datetime = timezone.localize(end_time)
        
        while start <= end: 
            day = int(start.timestamp())
            start = start + datetime.timedelta(days=1)

            response = urllib.request.urlopen('https://api.darksky.net/forecast/{}/{},{},{}?exclude=currently,daily,flags'.format(darksky_api_key, latitude, longitude, day)).read()
            output = json.loads(response)['hourly']['data']

            for item in output:
                time.append(item['time'])
                temperature.append(item['temperature'])
                cloudcover.append(item['cloudCover'])
                summary.append(item['summary'])

        temp_cloud_df = pd.DataFrame({'time':time, 'temperature':temperature,'clouds':cloudcover,'clds':summary})
        temp_cloud_df['time'] = pd.to_datetime(temp_cloud_df['time'], unit='s').dt.tz_localize('utc').dt.tz_convert(timezone).dt.tz_localize(None)
        temp_cloud_df['temperature'] = (temp_cloud_df['temperature'] - 32) * 5/9

    else: 
        print('Sorry, {} source has not been implemented yet.'.format(source))

    # logger.info(f"Cloud Cover: {temp_cloud_df}")
    return temp_cloud_df