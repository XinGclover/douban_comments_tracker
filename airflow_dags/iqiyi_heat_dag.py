from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


PROJECT_PATH = '/zhaoxuelu_tracker'


default_args = {
    'owner': 'cindy',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='iqiyi_heat_dag',
    default_args=default_args,
    description='Capture the popularity of IQIYI every 5 minutes',
    schedule_interval='*/10 * * * *',  # Every 5 minutes 
    start_date=datetime(2025, 6, 23),
    catchup=False,
    max_active_runs=1,
    tags=['iqiyi', 'heat','zhaoxuelu'],
) as dag:

    run_heat = BashOperator(
        task_id='run_iqiyi_heat_scraper',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/iqiyi/heat_scraper.py'
    )

