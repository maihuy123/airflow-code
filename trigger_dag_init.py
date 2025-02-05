from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

dag = DAG(
    default_args=default_args,
    dag_id="trigger_dag_init",
    schedule_interval="@once",
)

start_dag_nofi = BashOperator(
    task_id="start_dag_nofi",
    bash_command='echo "Start DAG"',
    dag=dag,
)

trigger_dag_1_task = TriggerDagRunOperator(
    task_id="trigger_dag_1",
    trigger_dag_id="dag_sync_1",
    wait_for_completion=True,
    poke_interval=60,
    failed_states=[State.FAILED],
    dag=dag,
)

trigger_dag_2_task = TriggerDagRunOperator(
    task_id="trigger_dag_2",
    trigger_dag_id="dag_sync_2",
    wait_for_completion=True,
    poke_interval=60,
    failed_states=[State.FAILED],
    dag=dag,
)

end_dag_nofi = BashOperator(
    task_id="end_dag_nofi",
    bash_command='echo "End DAG"',
    dag=dag,
)

start_dag_nofi >> [trigger_dag_1_task, trigger_dag_2_task] >> end_dag_nofi
