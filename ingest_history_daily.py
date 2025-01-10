import requests
import pandas as pd 
from pandas import json_normalize
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine , inspect
import psycopg2
import re

load_dotenv()

api_key = os.getenv('weather_api_key')
base_url = 'http://api.weatherapi.com/v1'
history_url = base_url + "/history.json"

db_name = os.getenv('db_name')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')

capitals = [
    "Johor Bahru", 
    "Alor Setar", 
    "Kota Bharu", 
    "Melaka", 
    "Seremban", 
    "Kuantan", 
    "George Town", 
    "Ipoh", 
    "Kangar", 
    "Kota Kinabalu", 
    "Kuching", 
    "Shah Alam", 
    "Kuala Terengganu",
    "Kuala Lumpur" 
]
dates_string = [(datetime.now() - timedelta(day)).strftime("%Y-%m-%d") for day in range(1,9)]
dates = [(datetime.now() - timedelta(day)) for day in range(1,9)]

def get_daily_history():
    for capital in capitals:
        for date in dates_string:
            params = {'key': api_key, 'q': capital , 'dt': date}
            response = requests.get(history_url , params=params)
            day_history_data = response.json()
            day_raw_data = pd.json_normalize(day_history_data)

            days_df = pd.json_normalize(day_raw_data['forecast.forecastday'][0][0]['day'])
            days_df['date'] = day_raw_data['forecast.forecastday'][0][0]['date']
            days_df['location'] = day_raw_data['location.name']
            days_df = days_df.rename(columns={"condition.text":"condition"} , inplace=False).drop(columns=['condition.icon','condition.code'] , axis=1)

            capital = re.sub(r'\s+', '_', capital)
            with engine.begin() as connection:
                days_df.to_sql(f"{capital}_daily" , if_exists='append' , index=False , con=connection)

get_daily_history()