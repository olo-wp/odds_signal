from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': '<olo>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


def greet(ti, age):
    first_name = ti.xcom_pull(task_ids='get_name', key = 'first_name')
    second_name = ti.xcom_pull(task_ids='get_name', key = 'second_name')
    print(f"Hello World, my name is {first_name} {second_name} and i'm {age} years old.")


def getName(ti):
    ti.xcom_push(key='first_name', value='bambi')
    ti.xcom_push(key='second_name', value='IRL')


with DAG(
        default_args=default_args,
        dag_id='first_python_dag_v5',
        description='First Python DAG',
        schedule_interval='@daily',
        start_date=datetime(2024, 9, 7),
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age': 22},
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=getName,
    )
    task2 >> task1