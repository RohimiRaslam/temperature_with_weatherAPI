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

def transform_history_hourly():
    # create cleaned table for hourly separated by states
    for name in inspector.get_table_names():    
        if not "cleaned" in name:
            if "hourly" in name:
                transformation_query_hourly = f"""
                    drop table if exists {name}_cleaned;
                    
                    create table {name}_cleaned as
                    select distinct on (date,time) *
                    from {name};
                    """
                with engine.begin() as connection:
                    connection.execute(text(transformation_query_hourly))
            else:
                continue

    # create list of cleaned hourly table names
    cleaned_hourly_tables = []
    for name in inspector.get_table_names():
        if not "all_states_hourly_cleaned" in name:
            if "cleaned" in name:
                if "hourly" in name:
                    cleaned_hourly_tables.append(name)
                else:
                    continue

    # append cleaned hourly table
    hourly_cleaned_union_string = ""
    for i, name in enumerate(cleaned_hourly_tables):
        if cleaned_hourly_tables[i] is not cleaned_hourly_tables[-1]:
            hourly_cleaned_union_string += (f"select * from {name} union ")
        if cleaned_hourly_tables[i] is cleaned_hourly_tables[-1]:
            hourly_cleaned_union_string += (f"select * from {name}")
    hourly_cleaned_query_string = f"drop table if exists all_states_hourly_cleaned; create table all_states_hourly_cleaned as with cte as ({hourly_cleaned_union_string}) select * from cte;"

    with engine.begin() as connection:
        connection.execute(text(hourly_cleaned_query_string))

transform_history_hourly()