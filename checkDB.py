import psycopg2 as db
import pandas as pd
import json
SCHEMA_FIELDS = "/home/huy/airflow/dags/config/development.json"

config = {
    "dataset": json.load(open(SCHEMA_FIELDS)).get("sale_dataset"),
    "project_id": json.load(open(SCHEMA_FIELDS)).get("project_id"),
    "bucket": json.load(open(SCHEMA_FIELDS)).get("bucket_sale"),
}
print(config["bucket"])