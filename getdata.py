from airflow import DAG
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from airflow.operators.empty import EmptyOperator


defaul_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_postgres_sql(ds, **kwargs):
    import jinja2
    BASE_DIR = Path(__file__).resolve().parent
    hook = PostgresHook(postgres_conn_id='ShoppingDB')
    sql_file = f'{BASE_DIR}/resource/test.sql'
    params = kwargs['params']
    params['ds'] = ds
    with open(sql_file, 'r') as f:
        template = jinja2.Template(f.read())
    sql = template.render(
        select=params.get('select', '*'),
        table=params.get('table'),
        where=params.get('where').replace('{{ ds }}', ds),  
        limit=params.get('limit')
    )
    print(f"Generated SQL:\n{sql}")
    data = hook.get_pandas_df(sql)
    print(data.head(10))
    


with DAG(
    default_args=defaul_args,
    schedule='@daily',
    catchup=False,
    dag_id='test_sql',
) as dag:
    dummpu_task = EmptyOperator(
        task_id='dummy_task',
    )
    check_sql_implention = PythonOperator(
        task_id='check_sql_implention',
        python_callable=load_postgres_sql,
        params={
            "select": 'sale_id, store_id, product_id, sale_date, quantity_sold, sale_amount',
            "table": 'sales',
            "where": "sale_date = '{{ ds }}'",
            "limit": None
        }
    )
    dummpu_task >> check_sql_implention