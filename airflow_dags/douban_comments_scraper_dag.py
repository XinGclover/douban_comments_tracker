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
    dag_id='douban_comments_scraper_dag',
    default_args=default_args,
    description='Scheduled crawling of Douban comments', 
    schedule_interval='0 * * * *',  # Every hour at minute 0 
    start_date=datetime(2025, 6, 23),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'comments','zhaoxuelu'],
) as dag:

    run_scraper = BashOperator(
        task_id='run_douban_scraper',
        bash_command= f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_comments_scraper.py'
    )
