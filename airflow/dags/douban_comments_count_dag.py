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
    dag_id='douban_comments_count_dag',
    default_args=default_args,
    description='Daily statistics of Douban comments',
    schedule_interval=timedelta(hours=6),  # Every 6 hours
    start_date=datetime(2025, 6, 23, 8, 0, 0, tzinfo=stockholm_tz),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'comments','zhaoxuelu']
) as dag:

    run_count = BashOperator(
        task_id='run_comments_count',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_comments_count.py'
    )

    run_segmentation = BashOperator(
        task_id='run_segmentation',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/analysis/jieba_words.py'
    )

    run_group_topics = BashOperator(
       task_id='run_group_topics',
       bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_group_topics_scraper.py'
    )

    run_count >> run_segmentation >> run_group_topics

