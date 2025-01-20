from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from pathlib import Path
from datetime import datetime, timedelta

def load_postgres_to_csv(table_name,jobs,execute_date,config):
    hook = PostgresHook(postgres_conn_id=config.get("postgres_conn_id"))

    BASEDIR = Path(__file__).resolve().parent.parent.parent

    sql_file_path = f'/{BASEDIR}/resource/postgrest/{jobs}_{table_name}.sql'
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_query = sql_file.read()
    except FileNotFoundError:
        print(f"File {sql_file_path} not found")
        return
    sql_query = sql_query.format(execute_date = execute_date)
    csv_file_path = f"/tmp/{jobs}_{table_name}_{execute_date}.csv"
    print(csv_file_path)
    data = hook.get_pandas_df(sql_query)
    data.to_csv(csv_file_path, index=False)

def load_csv_to_gcs(table,jobs,execute_date,config):
    gcs_hook = GCSHook(gcp_conn_id=config.get("gcs_conn_id"))
    csv_file_path = f"/tmp/{jobs}_{table.get('table_name')}_{execute_date}.csv"
    gcs_bucket = config.get("bucket")
    gcs_object = f"{jobs}_{table.get('table_name')}_{execute_date}.csv"
    print(table.get('schema'))
    gcs_hook.upload(
        bucket_name=gcs_bucket, 
        object_name=gcs_object, 
        filename=csv_file_path)

def load_to_bigquery(table,jobs,execute_date,config):
    bucket_name = "{}".format(config["bucket"])
    source_uri = f"gs://{bucket_name}/{jobs}_{table.get('table_name')}_{execute_date}.csv"
    print(table.get('schema'))
    return {
        "load": {
            "sourceUris": [source_uri],
            "destinationTable": {
                "projectId": "{}".format(config["project_id"]),
                "datasetId": "{}".format(config["dataset"]),
                "tableId": table.get('table_name'),
            },
            "writeDisposition": "WRITE_APPEND",
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "schema": {"fields": table.get('schema')},
        }
    }
    

def generate_dag_daily(params, table, defaul_args):
    dag = DAG(
        default_args=defaul_args,
        dag_id=f"load_daily_table_{table.get('table_name')}_to_bigquery",
        schedule=table.get("schedule"),
        catchup=table.get("catchup"),
        dagrun_timeout=table.get("dag_run_timeout"),
    )

    for job_name in table.get("tasks"):
        load_postgres_to_csv_task = PythonOperator(
            task_id=f"load_postgres_to_csv_{job_name}_{table.get('table_name')}",
            python_callable=load_postgres_to_csv,
            op_args=[table.get('table_name'),job_name, "{{ ds }}", params],
            dag=dag,
        )

        load_csv_to_gcs_task = PythonOperator(
            task_id=f"load_csv_to_gcs_{job_name}_{table.get('table_name')}",
            python_callable=load_csv_to_gcs,
            op_args=[table,job_name, "{{ ds }}", params],
            dag=dag,
        )

        load_to_bigquery_task = BigQueryInsertJobOperator(
            task_id=f"load_to_bigquery_{job_name}_{table.get('table_name')}",
            configuration=load_to_bigquery(table,job_name, "{{ ds }}", params),
            gcp_conn_id='google_cloud_defaults',
            dag=dag,
            
        )

        chain(load_postgres_to_csv_task, load_csv_to_gcs_task, load_to_bigquery_task)
        
    return dag

    