from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025,1,22),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'history_weather',
    default_args=default_args,
    schedule_interval='30 23 * * *',  # runs daily at 2330
    catchup=False,
) as dag:
    
    daily_script_task = BashOperator(task_id='ingest_daily' , bash_command='python /opt/airflow/scripts/ingest_history_daily.py')
    hourly_script_task = BashOperator(task_id='ingest_hourly' , bash_command='python /opt/airflow/scripts/ingest_history_hourly.py')

daily_script_task 
hourly_script_task 