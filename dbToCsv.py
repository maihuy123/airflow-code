import psycopg2 as db
import pandas as pd
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def postgres_to_csv(**kwargs):
    execution_date = kwargs['execution_date'].date()
    conn_string = 'dbname=shopping user=admin host=192.168.27.82 password=admin'
    conn = db.connect(conn_string)
    query = 'select * from sales where sale_date = \'2024-12-20\''
    print('hello', execution_date)
    data = pd.read_sql(query, conn)
    output_file = '/tmp/sales_2024-12-20.csv'
    data.to_csv(output_file, index=False)
    conn.close()

with DAG(
    dag_id='dbToCsv',
    schedule='@daily',
    catchup=False,
    default_args=default_args,
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_csv',
        python_callable=postgres_to_csv,
    )
   
    
    task1
