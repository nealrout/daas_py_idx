import psycopg2
import datetime
from main import config

configs = config.get_configs()


def setup_connection():
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
            # Convert to ISO 8601 format, ensuring UTC
            # record[key] = value.astimezone(datetime.timezone.utc).isoformat()
            # Convert to UTC, remove microseconds beyond 3 decimals, and ensure 'Z' timezone format
            record[key] = value.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return record
