from datetime import datetime, timedelta
import json
from util.env_variables import get_config, get_schema
from pathlib import Path
from factory.dag.generate_daily_dag import generate_dag_daily 

env_vars = get_config()
schema_vars = get_schema(Path(__file__).resolve())

defaul_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

params = {
    "dataset": env_vars.get("sale_dataset"),
    "project_id": env_vars.get("project_id"),
    "bucket": env_vars.get("bucket_sale"),
    "postgres_conn_id": env_vars.get("postgres_conn_id"),
    "gcs_conn_id": env_vars.get("gcs_conn_id"),
}


params_per_dag = [
    {
        "table_name": "sales",
        "sla": timedelta(minutes=30),
        "schedule": "@once",
        "catchup": False,
        "schema": schema_vars.get("sales").get("fields"),
        "tasks" : ["daily_insert"],
        "dag_run_timeout": timedelta(minutes=10),
        "postgres_sql": {
            "select" : 'sale_id, store_id, product_id, sale_date, quantity_sold, sale_amount',
            "from" : 'sales',
            "where" : "sale_date = '{{ ds }}'",
            "limit" : None
        }
        
    },
    {
        "table_name" : "shippment",
        "sla": timedelta(minutes=30),
        "schedule": "@daily",
        "catchup": False,
        "schema": schema_vars.get("shippment"),
        "tasks" : ["daily_insert"],
        "dag_run_timeout": timedelta(minutes=10),
        "postgres_sql": {
            "select" : None,
            "where" : None,
            "limit" : None
        }
    }
]

for table in params_per_dag:
    globals()[table.get("table_name")] = generate_dag_daily(params,table,defaul_args)
