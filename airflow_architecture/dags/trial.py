from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    return 'Hello World'

default_args = {
    'owner': '<olo>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        default_args=default_args,
        dag_id='fortuna_scraping',
        description='-',
        schedule_interval='@daily',
        start_date= days_ago(0) #datetime(2024, 9, 7),
) as dag:
    task1 = PythonOperator(
        task_id='scraper',
        python_callable=hello,
    )
    task1