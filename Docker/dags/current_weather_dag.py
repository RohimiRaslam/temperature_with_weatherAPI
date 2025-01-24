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
    'current_weather',
    default_args=default_args,
    schedule_interval='3 * * * *',  # Runs hourly at 3rd minute
    catchup=False,
) as dag:
    
    current_script_task = BashOperator(task_id='ingest_current' , bash_command='python /opt/airflow/scripts/ingest_current.py')

current_script_task