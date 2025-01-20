import json
from pathlib import Path
def get_config():
    BASE_DIR = Path(__file__).resolve().parent.parent
    CONFIG_FILE = BASE_DIR / 'config' / 'development.json'
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at {CONFIG_FILE}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {CONFIG_FILE}. Error: {str(e)}")

    
def get_schema(key):
    BASE_DIR = Path(__file__).resolve().parent.parent
    SCHEMA_FILE = BASE_DIR / 'config' / 'schema' / 'bigquery_schema.json'
    try:
        with open(SCHEMA_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at {SCHEMA_FILE}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {SCHEMA_FILE}. Error: {str(e)}")  
    


