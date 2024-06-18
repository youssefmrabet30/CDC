from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2

def populate_postgres():
    connection = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='postgres'
    )
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS my_table (
        id SERIAL PRIMARY KEY,
        data TEXT
    );
    """)
    cursor.execute("""
    INSERT INTO my_table (data)
    VALUES ('Sample data 1'), ('Sample data 2'), ('Sample data 3');
    """)
    connection.commit()
    cursor.close()
    connection.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('populate_postgres_dag', default_args=default_args, schedule_interval='@once') as dag:
    populate_task = PythonOperator(
        task_id='populate_postgres_task',
        python_callable=populate_postgres
    )
