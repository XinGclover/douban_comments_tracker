from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_DIR = Path(__file__).resolve().parent

PROJECT_PATH = DAG_DIR.parent.parent

stockholm_tz = pendulum.timezone("Europe/Stockholm")

default_args = {
    'owner': 'cindy',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='low_rating_collect_dag',
    default_args=default_args,
    description='Scheduled get Douban review collection of low rating users of Zhaoxuelu or others',
    schedule_interval=timedelta(hours=1),  # Every 2 hours
    start_date=datetime(2025, 7, 3, 8, 0, 0, tzinfo=stockholm_tz),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'comments','zhaoxuelu']
) as dag:

    run_scraper = BashOperator(
        task_id='run_user_rating',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_user_ratings.py'
    )

    run_filter = BashOperator(
        task_id='insert_high_rating_dramas',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/insert_high_rating_dramas.py'
    )

    run_scraper >> run_filter