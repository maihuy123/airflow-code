from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    default_args=default_args,
    schedule='00 11 * * MON',
    dag_id='checkDataItval',

) as dag:
    task_1 = EmptyOperator(task_id='task1')
    task_1