import psycopg2
import datetime
# from main import config
import json
import pandas as pd

# configs = config.get_configs()

def setup_connection(config):
    configs = config.get_configs()
    db_config = {
    "dbname": configs.DATABASE_NAME,
    "user": config.get_secret("DATABASE_USER"),
    "password": config.get_secret("DATABASE_PASSWORD"),
    "host": configs.DATABASE_HOST,
    "port": configs.DATABASE_PORT,
    "options": f"-c search_path={configs.DATABASE_SCHEMA}"
}
    
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    return conn, cursor

def convert_timestamptz_to_date(record):
    for key, value in record.items():
        if isinstance(value, datetime.datetime):
            if value is None or pd.isna(value) or value is pd.NaT: # Check if it's NaT
                record[key] = None  # Or any placeholder like ''
            else:
                # Convert to ISO 8601 format, ensuring UTC
                # record[key] = value.astimezone(datetime.timezone.utc).isoformat()
                # Convert to UTC, remove microseconds beyond 3 decimals, and ensure 'Z' timezone format
                record[key] = value.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return record

# def convert_jsonb(value):
#     if isinstance(value, dict) or isinstance(value, list):  # Handle native Python JSON structures
#         return json.dumps(value)
#     return value  # Return unchanged if not JSON

def convert_jsonb(value):
    """Convert JSONB field to a format compatible with Pandas and Solr."""
    if isinstance(value, str):  # If it's a JSON string, try to decode it
        try:
            decoded_value = json.loads(value)
            if isinstance(decoded_value, list):  # Ensure it's a list
                return decoded_value
            return value  # If not a list, return as-is
        except json.JSONDecodeError:
            return value  # Return as-is if it can't be decoded
    return value  # Return as-is if not a string
