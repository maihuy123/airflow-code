from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}
dag = DAG(
    default_args=default_args,
    dag_id="dag_sync_2",
    schedule_interval="@once",
)
alert_task_success = BashOperator(
    task_id="alert_task_success",
    bash_command='echo "dag2 success"',
    dag=dag,
)
