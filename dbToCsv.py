import psycopg2 as db
import pandas as pd
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.triggers.external_task import TaskStateTrigger
from airflow.operators.python import BranchPythonOperator
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def postgres_to_csv(**kwargs):
    execution_date = kwargs['execution_date'].date()
    conn_string = 'dbname=shopping user=admin host=localhost password=admin'
    conn = db.connect(conn_string)
    query = f"select * from sales where sale_date = '{execution_date}'"
    print('hello', execution_date)
    data = pd.read_sql(query, conn)
    output_file = f'/tmp/sales_{execution_date}.csv'
    data.to_csv(output_file, index=False)
    conn.close()

def check_data_not_null(**kwargs):
    execution_date = kwargs['execution_date'].date()
    file_path = f'/tmp/sales_{execution_date}.csv'
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            data = file.read().strip()
            if data:
                return 'upload_to_gcs'        
    return 'end_'

with DAG(
    dag_id='dbToCsv',
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    sla_miss_callback= timedelta(minutes=1),
    
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_csv',
        python_callable=postgres_to_csv,
    )
    task2 = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/sales_{{ ds }}.csv',
        dst='sales_{{ ds }}.csv',
        bucket='shippment_bucket',
    )
    task3 = BranchPythonOperator(
        task_id='check_data_not_null',
        python_callable=check_data_not_null,
    )
    task1 >> task3
    endBranch = PythonOperator(
        task_id='end_',
        python_callable=lambda: print('No data to upload')
    )
    task3 >> [task2, endBranch]
    def check_data_in_csv(**kwargs):
        execution_date = kwargs['execution_date'].date()
        file_path = f'/tmp/sales_{execution_date}.csv'
        with open(file_path, 'r') as file:
            df = pd.read_csv(file)
            if df.empty:
                return 'end_'
            else:
                return 'triggerGCSToBigQuery'
            
    checkData = BranchPythonOperator(
        task_id='checkData_in_csv',
        python_callable=check_data_in_csv,
    )

    triggerGCSToBigQuery = TriggerDagRunOperator(
    task_id='triggerGCSToBigQuery',
    trigger_dag_id='cloudToBigQuery',
    conf={'execution_date': '{{ ds }}'},
    wait_for_completion=False,
   )

    task2 >> checkData
    checkData >> [triggerGCSToBigQuery, endBranch]



    
