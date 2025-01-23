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
    'weatherAPI',
    default_args=default_args,
    schedule_interval='@daily',  # Runs daily
    catchup=False,
) as dag:
    # Define the BashOperator
    # run_script_task = BashOperator(task_id='run_script' , bash_command='python /opt/airflow/scripts/hello.py')
    run_script_task = BashOperator(task_id='run_script' , bash_command='python /opt/airflow/scripts/ingest_history_daily.py')
    # list_task = BashOperator(task_id='ls' , bash_command='ls')
    # change_task = BashOperator(task_id='cd' , bash_command='cd ../')
    # check_dir = BashOperator(task_id='pwd' , bash_command='pwd')

run_script_task 