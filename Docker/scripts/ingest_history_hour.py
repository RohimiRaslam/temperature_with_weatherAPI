import requests
import pandas as pd 
from pandas import json_normalize
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine
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
    "johor bahru", 
    "alor setar", 
    "kota bharu", 
    "melaka", 
    "seremban", 
    "kuantan", 
    "george town", 
    "ipoh", 
    "kangar", 
    "kota kinabalu", 
    "kuching", 
    "shah alam", 
    "kuala terengganu",
    "kuala lumpur" 
]

dates = [(datetime.now() - timedelta(day)).strftime("%Y-%m-%d") for day in range(0,9)]

def get_hourly_history():
    for date in dates:
        for capital in capitals:
            params = {"key": api_key, "q": capital, "dt": date}
            try:
                response = requests.get(history_url, params=params)
                if response.status_code == 200:
                    # flatten the data
                    history_data = response.json()
                    raw_data = pd.json_normalize(history_data)
                    hours_df = pd.json_normalize(raw_data['forecast.forecastday'][0][0]['hour'])
                    
                    # transform the data
                    hours_df['location'] = raw_data['location.name']
                    hours_df['region'] = raw_data['location.region']
                    hours_df['country'] = raw_data['location.country']
                    hours_df = hours_df.ffill(axis=0)
                    
                    # saving into postgresql
                    capital = re.sub(r'\s+', '_', capital)

                    with engine.connect() as conn:
                        hours_df.to_sql(f"{capital}_hourly" , if_exists='append' , index=False , con=conn)

                else:
                    print(f"Error: Received unexpected status code {response.status_code} on {capital}_{date}")

            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")

get_hourly_history()
