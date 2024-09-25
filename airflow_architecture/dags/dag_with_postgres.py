import sys
from datetime import timedelta
from datetime import datetime

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

def print_table():
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
            """
            SELECT * FROM games_id
            JOIN games ON games.id = games_id.id;     
            """
                   )
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    cursor.close()
    connection.close()


def scrape_and_insert():
    df = fortuna_scrape.fortuna_scraping()

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    now = datetime.now()

    for record in df.itertuples(index=False, name=None):
        game, home, draw, away, game_date = record
        try:
            cursor.execute(
                """
                INSERT INTO games_id (game, game_date)
                VALUES (%s, %s) 
                ON CONFLICT (game, game_date) 
                DO UPDATE SET game = EXCLUDED.game
                RETURNING ID
                """,
                (game,game_date)
            )

            game_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO games (id,home, draw, away, odds_date)
                 VALUES (%s,%s, %s, %s, %s)
                """,
                (game_id, home, draw, away, now)
            )
        except Exception as e:
            print(f'Failed to insert record: {e}')
            continue
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
    create_table_games_id = SQLExecuteQueryOperator(
        task_id='create_table_games_id',
        conn_id=postgres_conn_id,
        sql="""
        CREATE TABLE IF NOT EXISTS games_id (
        id SERIAL PRIMARY KEY,
        game varchar(64),
        game_date timestamp,
        UNIQUE (game, game_date)
        );
        """
    )

    create_table_games = SQLExecuteQueryOperator(
        task_id='create_table_games',
        conn_id=postgres_conn_id,
        sql="""
        CREATE TABLE IF NOT EXISTS games (
        id int references games_id(id),
        home float,
        draw float,
        away float,
        odds_date timestamp,
        PRIMARY KEY (id,odds_date)
        );
        """
    )

    scrape_and_insert = PythonOperator(
        task_id='scrap',
        python_callable=scrape_and_insert,
    )

    print_postgres = PythonOperator(
        task_id='print_postgres',
        python_callable=print_table,
    )

    create_table_games_id >> create_table_games >> scrape_and_insert >> print_postgres
