from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pathlib import Path
import jinja2
import json
import pandas as pd
from airflow.operators.bash import BashOperator

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def load_postgres_to_csv(jobs, config, tables, **kwargs):
    execution_date = kwargs.get("execution_date").strftime("%Y_%m_%d")
    hook = PostgresHook(postgres_conn_id=config.get("postgres_conn_id"))
    sql_file_path = f"/{BASE_DIR}/resource/postgrest/{jobs}.sql"
    try:
        with open(sql_file_path, "r") as sql_file:
            sql_query = jinja2.Template(sql_file.read())
    except FileNotFoundError:
        print(f"File {sql_file_path} not found")
        return
    sql = sql_query.render(
        column=tables.get("postgres_sql").get("column"),
        table=tables.get("postgres_sql").get("table"),
        condition=tables.get("postgres_sql").get("condition"),
        limit=tables.get("postgres_sql").get("limit"),
    )

    csv_file_path = f"/tmp/{jobs}_{tables.get("table_name")}_{execution_date}.csv"
    print(f"Exporting data to {csv_file_path}")
    data = hook.get_pandas_df(sql)
    data.to_csv(csv_file_path, index=False)


def data_validation(job, tables, **kwargs):
    execution_date = kwargs.get("execution_date").strftime("%Y_%m_%d")
    try:
        csv_file_path = f"/tmp/{job}_{tables.get("table_name")}_{execution_date}.csv"
        data = pd.read_csv(csv_file_path)
        print(f"Found file {csv_file_path}")
    except FileNotFoundError:
        print(f"File {csv_file_path} not found")
        return f"file_not_found_{tables.get('table_name')}"
    if data.empty:
        print(f"Data validation for {job} is not OK")
        return f"data_not_valid_{tables.get('table_name')}"

    data.dropna(how="all", inplace=True)  # Remove fully empty rows
    data.dropna(
        thresh=len(data.columns) * 0.7, inplace=True
    )  # Keep rows with at least 70% data
    data.columns = (
        data.columns.str.strip().str.lower().str.replace(" ", "_")
    )  # Normalize column names

    return "trigger_data_to_gcs"


def generate_dag_daily(params, table, defaul_args):
    dag = DAG(
        default_args=defaul_args,
        dag_id=f"table_{table.get('table_name')}_to_csv",
        schedule=table.get("schedule"),
        catchup=table.get("catchup"),
        dagrun_timeout=table.get("dag_run_timeout"),
    )
    for job in table.get("tasks"):
        # save postgres data to csv
        load_postgres_to_csv_task = PythonOperator(
            task_id=f"load_postgres_to_csv_{table.get('table_name')}",
            python_callable=load_postgres_to_csv,
            op_args=[job, params, table],
            dag=dag,
        )
        # check data validation
        check_data_validation_task = BranchPythonOperator(
            task_id=f"check_data_validation_{table.get('table_name')}",
            python_callable=data_validation,
            op_args=[job, table],
            dag=dag,
        )
        # clean data
        # trigger dag load_data_to_gcs
        trigger_dag_load_gcp_task = TriggerDagRunOperator(
            task_id=f"trigger_data_to_gcs",
            trigger_dag_id=f"table_{table.get('table_name')}_to_cloud",
            conf=json.dumps(
                {
                    "table_name": table.get("table_name"),
                    "job": job,
                    "execution_date": "{{ execution_date.strftime('%Y_%m_%d') }}",
                }
            ),
            wait_for_completion=True,
            dag=dag,
        )
        # Error task
        file_not_found_task = BashOperator(
            task_id=f"file_not_found_{table.get('table_name')}",
            bash_command='echo "File not found"',
            dag=dag,
        )
        data_not_valid_task = BashOperator(
            task_id=f"data_not_valid_{table.get('table_name')}",
            bash_command='echo "Data not valid"',
            dag=dag,
        )

        load_postgres_to_csv_task >> check_data_validation_task
        check_data_validation_task >> [
            file_not_found_task,
            data_not_valid_task,
            trigger_dag_load_gcp_task,
        ]

    return dag
