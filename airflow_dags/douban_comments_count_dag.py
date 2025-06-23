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
    dag_id='douban_comments_count_dag',
    default_args=default_args,
    description='Daily statistics of Douban comments',
    schedule_interval=timedelta(hours=2),  # Every day at midnight 
    start_date=datetime(2025, 6, 23),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'comments','zhaoxuelu'],
) as dag:

    run_count = BashOperator(
        task_id='run_comments_count',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_comments_count.py'
    )
