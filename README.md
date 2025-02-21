# temperature_with_weatherAPI

## Introduction
This Project was done in order to store weather data from WeatherAPI. 

## Summary
Using 2 differnt APIs provided by the WeatherAPI site, current weather and historical weather data of all the previous 8 days before the current date is extracted, transformed and stored into PostgreSQL database. The extracted data then were cleaned further to ensure data quality, and making them usable for analysis. Inside the database, the cleaned data is then save in a separrate table which is ready and usable to be analyse. To visualizer the data, a dashboard consisting of the cleaned data was built using Grafana.

## Tools
**Extraction**: Python, Pandas, Requests
**Transformation**: SQL, Python
**Load**: PostgreSQL, SQLAlchemy
**Orchetration**: Apache Airflow
**API**: WeatherAPI
