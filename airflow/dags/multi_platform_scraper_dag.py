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
    dag_id='multi_platform_scraper_dag',
    default_args=default_args,
    description='Capture the parameters of weibo every day',
    schedule_interval='0 */8 * * *',  # Once every 8 hours
    start_date=datetime(2025, 7, 7, 8, 0, 0, tzinfo=stockholm_tz),
    catchup=False,
    max_active_runs=1,
    tags=['weibo','zhaoxuelu'],
) as dag:

    run_scraper = BashOperator(
        task_id='run_multi_platform_scraper',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 -m multi_platform_scraper.main'
    )