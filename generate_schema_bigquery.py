
from datetime import datetime, timedelta
import json
from factory.dag.generate_table import generate_dag


SCHEMA_FIELDS = "/home/huy/airflow/dags/config/schema/bigquery_schema.json"
CONFIG_FILE = "/home/huy/airflow/dags/config/development.json"


with open(SCHEMA_FIELDS, 'r') as f:
    SCHEMA_DEFINITIONS = json.load(f)

default_args ={
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

params = {
    "schema": SCHEMA_DEFINITIONS,
    "dataset": json.load(open(CONFIG_FILE)).get("sale_dataset"),
    "project_id": json.load(open(CONFIG_FILE)).get("project_id"),
    "bucket": json.load(open(CONFIG_FILE)).get("bucket_sale"),
}


for table_name in SCHEMA_DEFINITIONS.keys():
    globals()[table_name] = generate_dag(default_args, table_name, params)

        
