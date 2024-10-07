from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/opt/airflow/dags/my_module')
from my_module import test #, TS_spark

def hi():
    print('hi')

default_args = {
    'owner': '<olo>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        default_args=default_args,
        dag_id='fortuna_scraping_test_2',
        description='-',
        schedule_interval=timedelta(minutes=5),
        start_date= days_ago(0) #datetime(2024, 9, 7),
) as dag:
    task1 = PythonOperator(
        task_id='scraper',
        python_callable=test.test,
    )

    # stream_to_spark = PythonOperator(
    #     task_id='stream_to_spark',
    #     python_callable=TS_spark.send_to_spark()
    # )

    task1