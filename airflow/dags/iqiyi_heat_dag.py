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
    dag_id='iqiyi_heat_dag',
    default_args=default_args,
    description='Capture the popularity of IQIYI every 20 minutes',
    schedule_interval='*/20 * * * *',  # Every 20 minutes
    start_date=datetime(2025, 6, 23, 8, 0, 0, tzinfo=stockholm_tz),
    catchup=False,
    max_active_runs=1,
    tags=['iqiyi', 'heat','zhaoxuelu'],
) as dag:

    run_heat = BashOperator(
        task_id='run_iqiyi_heat_scraper',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/iqiyi/heat_scraper.py'
    )


    run_hot_search = BashOperator(
        task_id='run_iqiyi_hot_search',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/iqiyi/hot_search.py'
    )

    run_tv_rank = BashOperator(
        task_id='run_iqiyi_tv_rank',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/iqiyi/tv_rank.py'
    )

    run_heat >> run_hot_search >> run_tv_rank