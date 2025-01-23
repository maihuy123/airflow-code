from airflow import DAG
from datetime import datetime, timedelta
import json
from factory.dag.generate_table import load_csv_to_gcs, load_postgres_to_csv, load_to_bigquery
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA_FIELDS = "/home/huy/airflow/dags/config/schema/bigquery_schema.json"
CONFIG_FILE = "/home/huy/airflow/dags/config/development.json"

with open(SCHEMA_FIELDS, 'r') as f:
    schema = json.load(f)

config = {
    "schema": schema,
    "dataset": json.load(open(CONFIG_FILE)).get("sale_dataset"),
    "project_id": json.load(open(CONFIG_FILE)).get("project_id"),
    "bucket": json.load(open(CONFIG_FILE)).get("bucket_sale")
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='generate_table_bigquery_one_dag',
    default_args=default_args,
    schedule='@once',
    
) as dag:
    start_dag_nofi = BashOperator(
        task_id = 'start_dag_nofi',
        bash_command='echo "Start DAG"'
    )

    end_dag_nofi = BashOperator(
        task_id = 'end_dag_nofi',
        bash_command='echo "End DAG"',
        trigger_rule='all_success'
    )

    for table_name in schema.keys():
        load_postgres_to_csv_task = PythonOperator(
            task_id=f'load_postgres_to_csv_{table_name}',
            python_callable=load_postgres_to_csv,
            op_args=[table_name]
        )

        load_csv_to_gcs_task = PythonOperator(
            task_id=f'load_csv_to_gcs_{table_name}',
            python_callable=load_csv_to_gcs,
            op_args=[table_name, config]
        )

        load_to_bigquery_task = BigQueryInsertJobOperator(
            task_id=f'load_to_bigquery_{table_name}',
            configuration=load_to_bigquery(table_name, config),
            gcp_conn_id='google_cloud_defaults',
        )

        start_dag_nofi >> load_postgres_to_csv_task >> load_csv_to_gcs_task >>  load_to_bigquery_task >> end_dag_nofi

  

        



