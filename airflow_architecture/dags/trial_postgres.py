from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': '<olo>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'trial_postgres_dag_v2',
    default_args=default_args,
    start_date=days_ago(0),
    schedule_interval='0 0 * * *',
) as dag:
    task1 = PostgresOperator(
        task_id='postgres_task1',
        postgres_conn_id='posgres_localhost',
        sql= """
            CREATE TABLE IF NOT EXISTS dag_runs(
                dt date,
                dag_id varchar,
                primary key(dt,dag_id)
            )
        """
    )
    task2 = PostgresOperator(
        task_id='postgres_task2',
        postgres_conn_id='posgres_localhost',
        sql= """
            INSERT INTO dag_runs(dt,dag_id) VALUES('{{dt}}','{{dag.dag_id}}')
        """
    )
