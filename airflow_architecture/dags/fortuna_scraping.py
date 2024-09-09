from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from odds_signal.fortuna_scrape import fortuna_scraping

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
        start_date=datetime(2024, 9, 7),
) as dag:
    task1 = PythonOperator(
        task_id='scraper',
        python_callable=fortuna_scraping,
    )
    #task1