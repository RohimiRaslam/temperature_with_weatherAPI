import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
api_key = os.getenv('weather_api_key')
base_url = 'http://api.weatherapi.com/v1'

capitals = [
    "Johor Bahru", 
    "Alor Setar", 
    "Kota Bharu", 
    "Malacca City", 
    "Seremban", 
    "Kuantan", 
    "George Town", 
    "Ipoh", 
    "Kangar", 
    "Kota Kinabalu", 
    "Kuching", 
    "Shah Alam", 
    "Kuala Terengganu",
    "Kuala Lumpur", 
]

def get_hourly_history():
    folder_path = 'hourly_data'
    os.makedirs(folder_path, exist_ok=True)
    history_url = base_url + "/history.json"

    dates = [(datetime.now() - timedelta(day)).strftime("%Y-%m-%d") for day in range(1,9)]

    for date in dates:
        for capital in capitals:
            params = {"key": api_key, "q": capital, "dt": date}
            try:
                response = requests.get(history_url, params=params)
                if response.status_code == 200:
                    history_data = response.json()
                    day = history_data['forecast']['forecastday'][0]['day']        
                    hourly = history_data['forecast']['forecastday'][0]['hour']
                    
                    hourly_dict = {}
                    
                    for d in hourly:
                        for key, value in d.items():
                            if key in hourly_dict:
                                hourly_dict[key].append(value)
                            else:
                                hourly_dict[key] = [value]
                    df = pd.DataFrame(hourly_dict)

                    file_name = f"{capital}_{date}"
                    file_path = os.path.join(folder_path , file_name)

                    if os.path.exists(file_path):
                        print(f"{file_path} is already existed, skipping...")
                        continue
                    else:
                        print(f'saving {file_path}...')
                        df.to_csv(file_path , index=False, header=True, encoding=None)
                
                # elif response.status_code == 404:
                #     print("Error: Not Found (404)")
                elif response.status_code == 401:
                    print("Error: API key provided is invalid.")
                else:
                    print(f"Error: Received unexpected status code {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")

get_hourly_history()
