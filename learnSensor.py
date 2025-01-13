from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='learnSensor',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World"',
    )
    task2 = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Goodbye World"',
    )
    task1 >> task2



default_args2 = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='learnSensor2',
    default_args=default_args2,
    schedule_interval='*/2 * * * *',
    catchup=False,
) as dag2:
    check_hello = ExternalTaskSensor(
        task_id = 'check_hello',
        external_dag_id='learnSensor',
        external_task_id='print_hello',
        mode =  'reschedule',
        poke_interval=30,
        timeout = 30,
    )