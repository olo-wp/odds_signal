import sys

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow/dags/my_module')
from my_module import test

default_args = {
    'owner': '<olo>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        default_args=default_args,
        dag_id='fortuna_scraping_test_5',
        description='-',
        schedule_interval='@daily',
        start_date=days_ago(0)
) as dag:
    task1 = PythonOperator(
        task_id='scraper',
        python_callable=test.test,
    )
