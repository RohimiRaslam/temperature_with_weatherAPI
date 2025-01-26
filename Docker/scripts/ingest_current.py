import requests
import pandas as pd 
from pandas import json_normalize
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import re

def get_current():
    load_dotenv(override=True)

    api_key = os.getenv('weather_api_key')
    db_name = os.getenv('db_name')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')
    capitals = ["johor bahru", "alor setar", "kota bharu",  "melaka", "seremban", "kuantan", "george town", "ipoh", "kangar", "kota kinabalu", "kuching", "shah alam", "kuala terengganu", "kuala lumpur"]
    current_url = 'http://api.weatherapi.com/v1/current.json'
    current_df = pd.DataFrame()

    for capital in capitals:
        params = {'key' : api_key , 'q': capital , 'aqi' : 'no'}
        raw_data = requests.get(current_url , params=params).json()
        raw_location = json_normalize(raw_data['location'])
        location = raw_location[['name' , 'region' , 'localtime']]
        raw_current = json_normalize(raw_data['current'])
        current = raw_current[['temp_c' , 'is_day' , 'wind_kph' , 'precip_mm' , 'humidity' , 'cloud' , 'feelslike_c' , 'heatindex_c' , 'uv' , 'condition.text']]
        
        df = pd.concat([location , current] , axis=1)
        df = df.rename(columns={"condition.text":"condition"} , inplace=False)
        df['localtime'] = pd.to_datetime(df['localtime'])
        df['date'] = df['localtime'].dt.date
        df['time'] = df['localtime'].dt.time
        df = df.drop('localtime' , axis=1)
        
        current_df = pd.concat([current_df , df] , ignore_index=True , axis=0)
        
        
    with engine.connect() as connection:
        current_df.to_sql(f'weather_current' , if_exists='append' , index=False , con=connection)

get_current()


# to add a funtion to check for yesterday's current weather in the database then delete it
# 1. connect to database
# 2. check the date
# 3. delete if it yesterday's