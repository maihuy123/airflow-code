from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import json
from pathlib import Path
import jinja2

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def load_csv_to_gcs(config, table, **kwargs):
    jobs = kwargs.get("dag_run").conf.get("job")
    execute_date = kwargs.get("dag_run").conf.get("execution_date")
    gcs_hook = GCSHook(gcp_conn_id=config.get("gcs_conn_id"))
    csv_file_path = f"/tmp/{jobs}_{table.get('table_name')}_{execute_date}.csv"

    try:
        csv_file_path = f"/tmp/{jobs}_{table.get('table_name')}_{execute_date}.csv"
        gcs_bucket = config.get("bucket")
        gcs_object = f"{jobs}_{table.get('table_name')}_{execute_date}.csv"
        print(table.get("schema"))
        gcs_hook.upload(
            bucket_name=gcs_bucket, object_name=gcs_object, filename=csv_file_path
        )
    except FileNotFoundError:
        print(f"File {csv_file_path} not found")
        return f'file_not_found_{table.get("table_name")}'


def create_table_bigquery(table, jobs, config):

    sql_file_path = (
        f'/{BASE_DIR}/resource/bigquery/{jobs}/{table.get("table_name")}.sql'
    )
    with open(sql_file_path, "r") as sql_file:
        sql_query = jinja2.Template(sql_file.read())
    sql = sql_query.render(
        project=config.get("project_id"),
        dataset=config.get("dataset"),
        jobs=config.get("jobs"),
    )
    return {"query": {"query": sql, "useLegacySql": False}}


def load_to_bigquery(table, jobs, config, execution_date):
    execution_date = execution_date.replace("-", "_")
    schema_path = f"{BASE_DIR}/resource/bigquery/{jobs}/schema.json"
    try:
        with open(schema_path, "r") as schema_file:
            schema = json.load(schema_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON schema: {e}")
    table_name = table.get("table_name")
    schema_table = schema.get(table_name)
    bucket_name = config.get("bucket")

    source_uri = f"gs://{bucket_name}/{jobs}_{table_name}_{execution_date}.csv"
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
            "schema": {"fields": schema_table.get("fields", [])},
        }
    }


def generate_dag_connect_gcp(params, table, defaul_args):
    dag = DAG(
        default_args=defaul_args,
        dag_id=f"table_{table.get('table_name')}_to_cloud",
        schedule=None,
        catchup=table.get("catchup"),
        dagrun_timeout=table.get("dag_run_timeout"),
    )
    for job in table.get("tasks"):
        # load data to gcs
        load_csv_to_gcs_task = PythonOperator(
            task_id=f"load_csv_to_gcs_{table.get('table_name')}",
            python_callable=load_csv_to_gcs,
            op_args=[params, table],
            dag=dag,
        )
        # create table in bigquery
        create_table_bigquery_task = BigQueryInsertJobOperator(
            task_id=f"create_table_bigquery_{table.get('table_name')}",
            configuration=create_table_bigquery(table, job, params),
            gcp_conn_id="google_cloud_defaults",
            dag=dag,
        )
        # load data to bigquery
        load_to_bigquery_task = BigQueryInsertJobOperator(
            task_id=f"load_to_bigquery_{table.get('table_name')}",
            configuration=load_to_bigquery(
                table,
                job,
                params,
                execution_date="{{ dag_run.conf['execution_date'] }}",
            ),
            gcp_conn_id="google_cloud_defaults",
            dag=dag,
        )
        load_csv_to_gcs_task >> create_table_bigquery_task >> load_to_bigquery_task

    return dag
