import requests
import pandas as pd 
from pandas import json_normalize
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
import re

def get_daily_history():
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
            params = {'key': api_key, 'q': capital , 'dt': date}
            raw_data = requests.get(history_url , params=params).json()
            history_data = pd.json_normalize(raw_data)

            days_df = pd.json_normalize(history_data['forecast.forecastday'][0][0]['day'])
            days_df['date'] = pd.to_datetime(history_data['forecast.forecastday'][0][0]['date'])
            days_df['location'] = history_data['location.name']
            days_df = days_df.rename(columns={"condition.text":"condition"} , inplace=False).drop(columns=['condition.icon','condition.code'] , axis=1)
            days_df = days_df[['location' , 'date' , 'maxtemp_c' , 'mintemp_c' , 'avgtemp_c' , 'avghumidity' , 'uv' , 'condition']]

            capital = re.sub(r'\s+', '_', capital)
            with engine.begin() as connection:
                days_df.to_sql(f"{capital}_daily" , if_exists='append' , index=False , con=connection)

get_daily_history()