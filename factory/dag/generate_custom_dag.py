from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from pathlib import Path
from datetime import datetime, timedelta
import jinja2
import json

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def load_postgres_to_csv(table_name,jobs,config,tables):
    hook = PostgresHook(postgres_conn_id=config.get("postgres_conn_id"))
    sql_file_path = f'/{BASE_DIR}/resource/postgrest/{jobs}.sql'
    try:
        with open(sql_file_path, 'r') as sql_file:
            sql_query = jinja2.Template(sql_file.read())
    except FileNotFoundError:
        print(f"File {sql_file_path} not found")
        return
    
    sql = sql_query.render(
        select=tables.get("postgres_sql").get("select"),
        table=tables.get("postgres_sql").get("from"),
        where=tables.get("postgres_sql").get("where"),
        limit=tables.get("postgres_sql").get("limit")
    )

    csv_file_path = f"/tmp/{jobs}_{table_name}.csv"
    print(csv_file_path+"hello")
    data = hook.get_pandas_df(sql)
    data.to_csv(csv_file_path, index=False)

def load_csv_to_gcs(table,jobs,config):
    gcs_hook = GCSHook(gcp_conn_id=config.get("gcs_conn_id"))
    csv_file_path = f"/tmp/{jobs}_{table.get('table_name')}.csv"
    gcs_bucket = config.get("bucket")
    gcs_object = f"{jobs}_{table.get('table_name')}.csv"
    print(gcs_object)
    gcs_hook.upload(
        bucket_name=gcs_bucket, 
        object_name=gcs_object, 
        filename=csv_file_path)

def create_table_bigquery(table,jobs,config):
    
    sql_file_path = f'/{BASE_DIR}/resource/bigquery/{jobs}/{table.get("table_name")}.sql'
    with open(sql_file_path, 'r') as sql_file:
        sql_query = jinja2.Template(sql_file.read())
    sql =  sql_query.render(
        project = config.get("project_id"),
        dataset = config.get("dataset"),
        jobs = config.get("jobs"),
    )
    return {
        "query" : {
            "query": sql,
            "useLegacySql": False
        }
    }

def load_to_bigquery(table,jobs,config):
    schema_path = f'{BASE_DIR}/resource/bigquery/{jobs}/schema.json'
    
    try:
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON schema: {e}")  
    table_name = table.get('table_name')  
    schema_table = schema.get(table_name)  
    bucket_name = config.get("bucket")
    
    source_uri = f"gs://{bucket_name}/{jobs}_{table_name}.csv"
    project_id = config.get("project_id")
    dataset_id = config.get("dataset")
    return {
        "load": {
            "sourceUris": [source_uri],
            "destinationTable": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": f"{jobs}_{table_name}",
            },
            "writeDisposition": "WRITE_APPEND",
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "schema": {"fields": schema_table.get('fields', [])},
        }
    }
    

def generate_dag_daily(params, table, defaul_args):
    dag = DAG(
        default_args=defaul_args,
        dag_id=f"load_custom_table_{table.get('table_name')}_to_bigquery",
        schedule=table.get("schedule"),
        catchup=table.get("catchup"),
        dagrun_timeout=table.get("dag_run_timeout"),
    )

    for job_name in table.get("tasks"):
        load_postgres_to_csv_task = PythonOperator(
            task_id=f"load_postgres_to_csv_{job_name}_{table.get('table_name')}",
            python_callable=load_postgres_to_csv,
            op_args=[table.get('table_name'),job_name, params,table],
            dag=dag,
        )

        load_csv_to_gcs_task = PythonOperator(
            task_id=f"load_csv_to_gcs_{job_name}_{table.get('table_name')}",
            python_callable=load_csv_to_gcs,
            op_args=[table,job_name, params],
            dag=dag,
        )

        create_table_bigquery_task = BigQueryInsertJobOperator(
            task_id=f"create_table_bigquery_{job_name}_{table.get('table_name')}",
            configuration=create_table_bigquery(table,job_name, params),
            gcp_conn_id='google_cloud_defaults',
            dag=dag,
        )

        load_to_bigquery_task = BigQueryInsertJobOperator(
            task_id=f"load_to_bigquery_{job_name}_{table.get('table_name')}",
            configuration=load_to_bigquery(table,job_name, params),
            gcp_conn_id='google_cloud_defaults',
            dag=dag,
            
        )

        chain(load_postgres_to_csv_task, load_csv_to_gcs_task, create_table_bigquery_task,load_to_bigquery_task)
        
    return dag

    