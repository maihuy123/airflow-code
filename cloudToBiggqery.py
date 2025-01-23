from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id='cloudToBigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    def log_execution_date(**kwargs):
     execution_date = kwargs.get("dag_run").conf.get("execution_date", kwargs.get("ds"))
     print(f"Execution date: {execution_date}")

    log_date_task = PythonOperator(
     task_id="log_execution_date",
     python_callable=log_execution_date,
     provide_context=True,
    )


    task_1 = GCSToBigQueryOperator(
        task_id = "load_to_bigquery",
        bucket='shippment_bucket',
        source_objects=["sales_{{ dag_run.conf.execution_date or ds }}.csv"],
        destination_project_dataset_table='shipment_dataset_huy.sales_{{ dag_run.conf.execution_date or ds }}_staging',

        schema_fields=[
            {"name": "sale_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "store_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
            {"name": "sale_amount", "type": "NUMERIC", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",

    
    )
    log_date_task >> task_1
    task_2 = BigQueryInsertJobOperator(
        task_id="insert_job",
        configuration={
            "query": {
                "query": """
                    SELECT
                        sale_id,
                        store_id,
                        product_id,
                        sale_date,
                        quantity_sold,
                        sale_amount,
                        quantity_sold + sale_amount AS return_sale
                    FROM
                        `shipment_dataset_huy.sales_{{dag_run.conf.execution_date}}_staging`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "my-second-project-445300",
                    "datasetId": "shipment_dataset_huy",
                    "tableId": "sales_{{dag_run.conf.execution_date}}",
                },
                "writeDisposition": "WRITE_TRUNCATE", 
            }
        }
    )
    task_3 = BigQueryInsertJobOperator(
    task_id="drop_staging_table",
    configuration={
        "query": {
            "query": """
                DROP TABLE `shipment_dataset_huy.sales_{{dag_run.conf.execution_date}}_staging`
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_default",
    location="US",
    )  
    task_1 >> task_2 >> task_3
