# import libraries
import pandas as pd 
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import *

# create a connection to db
load_dotenv(override=True)

db_name = os.getenv('db_name')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
inspector  = inspect(engine)

def transform_history_daily():
    # create cleaned tables by states
    for name in inspector.get_table_names():    
        if "cleaned" not in name and "daily" in name:
            transformation_query_daily = f"""
                drop table if exists {name}_cleaned;

                create table {name}_cleaned as
                select distinct on (date) *
                from {name};
                """
            with engine.begin() as connection:
                connection.execute(text(transformation_query_daily))
        else:
            continue

    # create list of cleaned daily table names

    cleaned_daily_tables = [i for i in inspector.get_table_names() if "all_states_daily_cleaned" not in i and "cleaned" in i and "daily" in i]

    # create a table of all states 
    daily_cleaned_union_string = ""
    for i, name in enumerate(cleaned_daily_tables):
        if cleaned_daily_tables[i] is not cleaned_daily_tables[-1]:
            daily_cleaned_union_string += (f"select * from {name} union ")
        elif cleaned_daily_tables[i] is cleaned_daily_tables[-1]:
            daily_cleaned_union_string += (f"select * from {name}")
    daily_cleaned_query_string = f"drop table if exists all_states_daily_cleaned; create table all_states_daily_cleaned as with cte as ({daily_cleaned_union_string}) select * from cte;"

    # run query to clean
    with engine.begin() as connection:
        connection.execute(text(daily_cleaned_query_string))

    # run query to drop state's cleaned tables
    for i in cleaned_daily_tables:
        drop_query = f"drop table if exists {i}"
        with engine.begin() as connection:
            connection.execute(text(drop_query))

transform_history_daily()