import requests
import pandas as pd 
from pandas import json_normalize
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
import re

def get_hourly_history():
    load_dotenv(override=True)

    api_key = os.getenv('weather_api_key')
    db_name = os.getenv('db_name')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
    dates = [(datetime.now() - timedelta(day)).strftime("%Y-%m-%d") for day in range(1,9)]
    capitals = ["johor bahru", "alor setar", "kota bharu",  "melaka", "seremban", "kuantan", "george town", "ipoh", "kangar", "kota kinabalu", "kuching", "shah alam", "kuala terengganu", "kuala lumpur"]
    history_url = 'http://api.weatherapi.com/v1/history.json'

    for date in dates:
        for capital in capitals:
            params = {"key": api_key, "q": capital, "dt": date}
            raw_data = requests.get(history_url, params=params).json()
            history_data = pd.json_normalize(raw_data)
            hours_df = pd.json_normalize(history_data['forecast.forecastday'][0][0]['hour'])
            
            hours_df['location'] = history_data['location.name']
            hours_df['region'] = history_data['location.region']
            hours_df['country'] = history_data['location.country']
            hours_df = hours_df.ffill(axis=0)
            hours_df = hours_df.rename(columns={"condition.text":"condition"} , inplace=False).drop(columns=['condition.icon','condition.code'] , axis=1)
            hours_df['time'] = pd.to_datetime(hours_df['time'])
            hours_df['date'] = hours_df['time'].dt.date
            hours_df['time'] = hours_df['time'].dt.strftime("%H:%M")
            hours_df = hours_df[['location' , 'region' , 'country' , 'date' , 'time' , 'is_day' , 'temp_c' , 'humidity' , 'condition' , 'feelslike_c', 'wind_kph' , 'precip_mm'  , 'cloud'  , 'uv' , 'heatindex_c' , 'pressure_mb' , 'dewpoint_c']]
            
            capital = re.sub(r'\s+', '_', capital)

            with engine.connect() as conn:
                hours_df.to_sql(f"{capital}_hourly" , if_exists='append' , index=False , con=conn)

get_hourly_history()
