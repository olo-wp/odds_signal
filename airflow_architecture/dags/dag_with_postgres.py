import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


sys.path.append('/opt/airflow/dags/my_module')
from my_module import fortuna_scrape

postgres_conn_id = 'postgres_localhost'

default_args = {
    'owner': '<oli_troli>',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


def scrape_and_insert():
    df = fortuna_scrape.fortuna_scraping()

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for record in df.itertuples(index=False, name=None):
        cursor.execute(
            "INSERT INTO odds (game, home, draw, away, game_date) VALUES (%s, %s, %s, %s, %s)",
            record
        )
    connection.commit()
    cursor.close()
    connection.close()


with DAG(
        dag_id='dag_with_postgres',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        start_date=days_ago(0),
) as dag:


    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=postgres_conn_id,
        sql="""
        CREATE TABLE IF NOT EXISTS odds (
        game varchar,
        home float,
        draw float,
        away float,
        game_date timestamp,
        saved timestamp,
        PRIMARY KEY (game, game_date)
        );
        """
    )

    scrape_and_insert = PythonOperator(
        task_id='scrap',
        python_callable=scrape_and_insert,
    )

    print_postgres = SQLExecuteQueryOperator(
        task_id='print_postgres',
        conn_id=postgres_conn_id,
        sql="select * from odds;"
    )

    create_table >> scrape_and_insert >> print_postgres
