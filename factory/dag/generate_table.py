from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def load_postgres_to_csv(table_name):
    hook = PostgresHook(postgres_conn_id='ShoppingDB')
    sql_file_path = "/home/huy/airflow/dags/resource/Postgrest/select_all.sql"
    with open(sql_file_path, 'r') as sql_file:
        sql_query = sql_file.read()
    sql_query = sql_query.format(table_name = table_name)
    csv_file_path = f"/tmp/{table_name}.csv"

    data = hook.get_pandas_df(sql_query)
    data.to_csv(csv_file_path, index=False)


def load_csv_to_gcs(table_name,config):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_defaults')
    source_file_path = f"/tmp/{table_name}.csv"
    destination_blob_name = f"{table_name}.csv"
    bucket_name = '{}'.format(config["bucket"])
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=destination_blob_name,
        filename=source_file_path,
    )

def load_to_bigquery(table_name, params):
    schema = params['schema'][table_name]['fields']
    bucket_name = "{}".format(params["bucket"])
    source_uri = f"gs://{bucket_name}/{table_name}.csv"
    
    return {
        "load": {
            "sourceUris": [source_uri],
            "destinationTable": {
                "projectId": "{}".format(params["project_id"]),
                "datasetId": "{}".format(params["dataset"]),
                "tableId": table_name,
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "schema": {"fields": schema},
        }
    }

def generate_dag(default_args, table_name,params):
    dag = DAG(
        default_args=default_args,
        dag_id=f"load_table_{table_name}_to_bigquery",
        schedule="@once",
        catchup=False,
    )

    load_postgres_to_csv_task = PythonOperator(
        task_id=f"load_postgres_to_csv_{table_name}",
        python_callable=load_postgres_to_csv,
        op_args=[table_name],
        dag=dag,
    )

    load_csv_to_gcs_task = PythonOperator(
        task_id=f"load_csv_to_gcs_{table_name}",
        python_callable=load_csv_to_gcs,
        op_args=[table_name,params],
        dag=dag,
    )

    load_to_bigquery_task = BigQueryInsertJobOperator(
        task_id=f"load_to_bigquery_{table_name}",
        configuration=load_to_bigquery(table_name,params),
        gcp_conn_id='google_cloud_defaults',
        dag=dag,
    )
    list_tasks = [load_postgres_to_csv_task, load_csv_to_gcs_task, load_to_bigquery_task]
    chain(*list_tasks)
    return dag

    

    

