from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.branch import BaseBranchOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator
import random
from airflow.utils.trigger_rule import TriggerRule
default_args ={
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    default_args = default_args,
    schedule='@daily',
    dag_id = 'learnBranch',
    catchup=False,
) as dag:
    task1 = EmptyOperator(
        task_id = 'Start_dag'
    )
    option = ['branch_a', 'branch_b', 'branch_c']
    def choose_branch(**kwargs):
        """
        Randomly selects a branch from the available options.
        """
        choice = random.choice(option)
        print(f'Choosing branch: {choice}')
        return choice
    choose = BranchPythonOperator(
        task_id = 'choose_branch',
        python_callable = choose_branch,
    )
    task1 >> choose
    endBranch = EmptyOperator(
            task_id = 'end_',
            trigger_rule=TriggerRule.ONE_SUCCESS
        )
    for branch_name in option:
        branch = EmptyOperator(
            task_id = branch_name
        )
        choose >> branch >> endBranch
     


