import psycopg2 as db
import pandas as pd
import json

try:
    file_path = "/tmp/daily_custom_schema_sales_2025_02_03.csv"
    data = pd.read_csv(file_path)
    print(f"Found file {file_path}")
    print(data.head())
    if data.empty:
        print(f"Data validation for daily_custom_schema_sales is not OK")
except FileNotFoundError:
    print(f"File {file_path} not found")
