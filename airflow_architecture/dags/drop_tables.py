import sys
from datetime import timedelta

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': '<oli_troli>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_with_postgres',
        default_args=default_args,
        schedule_interval='@once',
        catchup=False,
        start_date=days_ago(0),
) as dag:
    drop_tables = SQLExecuteQueryOperator(
        task_id='drop_tables',
        postgres_conn_id='postgres_localhost',
        sql="""
        DROP TABLE IF EXISTS games_id CASCADE;
        DROP TABLE IF EXISTS games CASCADE;
        """,
    )
