import os
import subprocess
from datetime import datetime, time, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DAG_DIR = Path(__file__).resolve().parent

PROJECT_PATH = DAG_DIR.parent.parent

env = os.environ.copy()
env["PYTHONPATH"] = PROJECT_PATH

local_tz = pendulum.timezone("Asia/Shanghai")

default_args = {
    'owner': 'cindy',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_scraper_conditional():
    """ Run the scraper conditionally based on the current time.
    The scraper runs every 30 minutes during the time window of 19:00 to
    2:00, and on even hours at the top of the hour from 2:00 to 9:00.
    It runs only on the hour from 9:00 to 19:00.
    """
    now = datetime.now(tz=local_tz).time()
    current_hour = now.hour
    current_minute = now.minute

   # Check the current time and determine if the scraper should run
    if time(15, 0) <= now or now < time(3, 0):
        # 15:00 - 3:00,run every 30 minutes
        pass
    elif time(3, 0) <= now < time(9, 0):
        # 3:00 - 9:00, only run on even hours at the top of the hour
        if current_hour % 2 != 0 or current_minute != 0:
            raise AirflowSkipException("Skip run: 2-9 o'clock does not run on the hour of non-even hours")
    else:
        # 9:00 - 15:00， only run on the hour
        if current_minute != 0:
            raise AirflowSkipException("Skip run: 9-19 o'clock non-operation")


    subprocess.run(
        ["python3", "-m", "scraper.douban_comments_scraper"],
        check=True,
        cwd=PROJECT_PATH,
        env=env
        )

with DAG(
    dag_id='douban_comments_scraper_dag',
    default_args=default_args,
    description='Scheduled crawling of Douban comments',
    schedule_interval='*/30 * * * *',  # Every hour at minute 0
    start_date=datetime(2025, 6, 26, 8, 0, 0, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['douban', 'comments','zhaoxuelu'],
) as dag:

    run_scraper = PythonOperator(
        task_id='run_douban_scraper',
        python_callable=run_scraper_conditional,
    )

    run_filter = BashOperator(
        task_id='insert_low_rating',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/scraper/insert_low_rating_users.py'
    )

    send_to_kafka = BashOperator(
        task_id='run_producer_script',
        bash_command=f'PYTHONPATH={PROJECT_PATH} python3 {PROJECT_PATH}/kafka_pipeline/producer_send_comments.py',
    )


    run_scraper >> run_filter >> send_to_kafka