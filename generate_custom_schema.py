from datetime import datetime, timedelta
import json
from util.env_variables import get_config, get_schema
from pathlib import Path
from factory.dag.generate_custom_schema_dag import generate_dag_daily
from factory.dag.generate_dag_gcp import generate_dag_connect_gcp

env_vars = get_config()
schema_vars = get_schema(Path(__file__).resolve())

defaul_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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
        "schedule": "@daily",
        "catchup": False,
        "schema": schema_vars.get("sales").get("fields"),
        "tasks": ["daily_custom_schema"],
        "dag_run_timeout": timedelta(minutes=10),
        "postgres_sql": {
            "column": "sale_id, store_id, product_id, sale_date, quantity_sold, sale_amount",
            "table": "sales",
            "condition": "sale_date = '2025-02-02'",
            "limit": None,
        },
    },
    {
        "table_name": "shipment",
        "sla": timedelta(minutes=30),
        "schedule": "@daily",
        "catchup": False,
        "schema": schema_vars.get("shippment"),
        "tasks": ["daily_custom_schema"],
        "dag_run_timeout": timedelta(minutes=10),
        "postgres_sql": {
            "column": "shipment_id, customer_id, shipment_date",
            "table": "shipment",
            "condition": None,
            "limit": None,
        },
    },
]

for table in params_per_dag:
    daily_dag_name = f"{table.get('table_name')}_daily"
    connect_gcp_dag_name = f"{table.get('table_name')}_gcp"

    globals()[daily_dag_name] = generate_dag_daily(params, table, defaul_args)
    globals()[connect_gcp_dag_name] = generate_dag_connect_gcp(
        params, table, defaul_args
    )
