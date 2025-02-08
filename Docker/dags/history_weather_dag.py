from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Kuala_Lumpur")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025 , 1 , 22 , tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'history_weather',
    default_args=default_args,
    schedule_interval='30 00 * * *',  # runs daily at 0030
    catchup=False,
) as dag:
    
    extract_history_daily_script_task = BashOperator(task_id='extract_daily' , bash_command='python /opt/airflow/scripts/ingest_history_daily.py')
    extract_history_hourly_script_task = BashOperator(task_id='extract_hourly' , bash_command='python /opt/airflow/scripts/ingest_history_hourly.py')
    transform_history_daily_script_task = BashOperator(task_id='transform_daily' , bash_command='python /opt/airflow/scripts/transform_history_daily.py')
    transform_history_hourly_script_task = BashOperator(task_id='transform_hourly' , bash_command='python /opt/airflow/scripts/transform_history_hourly.py')

extract_history_daily_script_task >> transform_history_daily_script_task
extract_history_hourly_script_task  >> transform_history_hourly_script_task