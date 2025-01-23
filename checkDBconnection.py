from airflow import DAG
import psycopg2 as db
import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import  PostgresHook
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='checkDBconnection',
    schedule='@daily',
    catchup=False,
    default_args=default_args,
) as dag:
    def check_db_connection():
        hook = PostgresHook(postgres_conn_id='ShoppingDB')
        sql = 'select * from sales'
        data = hook.get_pandas_df(sql)
        print(data.head(10))
    check_connection = PythonOperator(
        task_id = 'check_db_connection',
        python_callable=check_db_connection,
    )
    

    
