import os
import json
from pathlib import Path

# Define the relative path from test_library.py to bigquery_schema.json
BASE_DIR = Path(__file__).resolve().parent.parent

print(BASE_DIR)