from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    default_args=default_args,
    schedule='@daily',
    dag_id='checkDateRun',
    catchup=True,
) as dag:
    task1 = DummyOperator(task_id='task1')
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "check the date:  {{ds}}"',
    )
