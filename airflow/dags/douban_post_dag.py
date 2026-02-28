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
    dag_id='douban_post_dag',
    default_args=default_args,
    description='Scheduled get Douban posts collection of Landy and others',
    schedule_interval="*/15 * * * *",  # Every 20 minutes
    start_date=datetime(2026, 2, 17, tzinfo=stockholm_tz),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'posts','Landy']
) as dag:

    run_scraper = BashOperator(
        task_id='run_posts_scraper',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/douban_post_scraper.py'
    )
    run_llm = BashOperator(
        task_id='run_LLM_analysis',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/analysisLLM/label_posts_with_llm.py'
    )

    run_scraper