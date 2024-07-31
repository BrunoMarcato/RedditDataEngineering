from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.abspath(__file__), '../../'))) # Adds the project main dir to path

from pipelines.reddit_pipeline import reddit_pipeline

# -------

default_args = {
    'owner': 'Bruno Marcato',
    'start_date': datetime(2024, 7, 25) # DAG start date
}

file_postfix = datetime.now().strftime("%Y%m%d") # Gets the current date and formats it in YYYYMMDD

dag = DAG(
    dag_id = 'etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # Sets the DAG execution interval to daily
    catchup=False, # Defines that the DAG should not execute past executions if they have not been executed.
    tags=['reddit', 'etl']
)

# -------

extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'filename': f'reddit_{file_postfix}',
        'subreddit': 'playstationbrasil',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

extract

# -------