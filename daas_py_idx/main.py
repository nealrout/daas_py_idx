"""
File: main.py
Description: Generic script that will take a domain, and if valid has one of two paths
            1.) Start event listener that will listen to postgresql event notify to get records that 
                were modified.  It then waits for X number of records or Y seconds before sending update
                to SOLR.  There are fully configurable.
            2.) Start a full load of all object of that type in the DB.
            
Author: Neal Routson
Date: 2025-02-07
Version: 0.1
"""
import sys
import os
import time
from bootstrap import bootstrap
import psycopg2
import pysolr
import json
import argparse
import inspect
import pyarrow as pa
import pandas as pd
import datetime
import importlib

logger, config = bootstrap()
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

def get_all(batch_start_ts=None, batch_end_ts=None):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()

        if batch_start_ts == None and batch_end_ts == None:
            cursor.execute(f"SELECT * FROM {DB_FUNC_GET}(%s);", [None])
        else:
            cursor.execute(f"SELECT * FROM {DB_FUNC_GET}(%s, %s, %s);", [None, batch_start_ts, batch_end_ts])
            
        data = cursor.fetchall()
        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        # Using pyarrow to convert fetched data to Arrow Table
        arrow_table = pa.Table.from_pandas(pd.DataFrame(data, columns=column_names))

    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

    return arrow_table

def get_by_id(json_data):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()
        cursor.execute(f"SELECT * FROM {DB_FUNC_GET_BY_ID}(%s, %s);", [json_data, None])
        data = cursor.fetchall()

        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        # Using pyarrow to convert fetched data to Arrow Table
        arrow_table = pa.Table.from_pandas(pd.DataFrame(data, columns=column_names))
    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")
    return arrow_table

def clean_event_notification_by_id(json_data):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()
        cursor.execute(f"SELECT * FROM {configs.DB_FUNC_CLEAN_EVENT_NOTIFICATION_BUFFER}(%s);", [json_data])
        conn.commit()
    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def apply_business_logic(arrow_table):
    try:
        logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
        #Not used yet.
        df = arrow_table.to_pandas()
        # df.loc[df["category"] == "Real Estate", "value"] *= 1.10
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")
        return pa.Table.from_pandas(df)
    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def update_solr(arrow_table, solr_url):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        solr = pysolr.Solr(solr_url)

        if arrow_table == None:
            logger.warning(f"No records passed to {inspect.currentframe().f_code.co_name}")
            return
        # Convert Arrow Table back to DataFrame and then to dict for SOLR
        solr_data = arrow_table.to_pandas().to_dict(orient="records")

        # Format records (timestamptz) to be compatible with solr
        for record in solr_data:
            record = convert_timestamptz_to_date(record)

        solr.add(solr_data)
        logger.info(f"Successfully updated {len(solr_data)} documents in SOLR.")
    except Exception as e:
        logger.error(f"Error in {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")
    
def process_all(solr_url):
    if not process_index_override():
        data = get_all()
        # processed_data = apply_business_logic(data)
        process_business_logic(module_name=f"business_logic.{DOMAIN.lower()}", data=data)
        update_solr(arrow_table=data, solr_url=solr_url)

def event_listener(solr_url):
    try:
        listener_conn, listener_cursor = setup_connection()
        listener_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        reader_conn, reader_cursor = setup_connection()

        listener_cursor.execute(f"LISTEN {DB_CHANNEL};")
        logger.info(f"Listening for {DB_CHANNEL} events...")

        notify_buffer = []
        notify_recover = []
        last_executed_time = time.time()

        # Recover updates made while this service was not running
        logger.info(f"Recovering buffered events before enabling listener")
        listener_cursor.execute(f"SELECT id, channel, payload FROM {configs.DB_FUNC_GET_EVENT_NOTIFICATION_BUFFER}(%s);", [DB_CHANNEL])
        buffered_events = listener_cursor.fetchall()

        for event in buffered_events:
            notification_id, channel, payload = event

            logger.debug(f"notification_id: {notification_id}, channel: {channel}, payload: {payload}")
            notify_recover.append(notification_id)
            notify_buffer.append(payload)

        logger.info(f"Recovering {len(notify_buffer)} buffered_events")

        while True:
            listener_conn.poll()
            while listener_conn.notifies:
                notify = listener_conn.notifies.pop(0)
                logger.debug(f"🔔 {DB_CHANNEL} Change Detected: {notify.payload}")
                notify_buffer.append(notify.payload)

            if len(notify_buffer) > int(IDX_BUFFER_SIZE) or (time.time() - last_executed_time >= int(IDX_BUFFER_DURATION)):
                if notify_buffer:

                    json_data = json.dumps({f"{IDX_FETCH_KEY}": notify_buffer}) 
                    data = get_by_id(json_data=json_data)
                    # processed_data = apply_business_logic(data)
                    update_solr(arrow_table=data, solr_url=solr_url)

                    # remove items from event_notification_buffer
                    json_data_recover= json.dumps({f"{configs.IDX_EVENT_RECOVER_KEY}": notify_recover}) 
                    clean_event_notification_by_id(json_data=json_data_recover)

                    # Reset tracking variables
                    notify_buffer.clear()
                    last_executed_time = time.time()

    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        listener_conn.close()
        listener_cursor.close()
        reader_conn.close()
        reader_cursor.close()

def convert_timestamptz_to_date(record):
    for key, value in record.items():
        if isinstance(value, datetime.datetime):
            # Convert to ISO 8601 format, ensuring UTC
            # record[key] = value.astimezone(datetime.timezone.utc).isoformat()
            # Convert to UTC, remove microseconds beyond 3 decimals, and ensure 'Z' timezone format
            record[key] = value.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return record

def process_business_logic(module_name, data):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        # Dynamically import the module
        module = importlib.import_module(module_name)
        
        # Check if the module has the expected function
        if hasattr(module, "process"):
            func = getattr(module, "process")
            func(data)  # Execute the function
        else:
            print(f"Module '{module_name}' does not contain a 'process' function.")
    except ModuleNotFoundError:
        print(f"Module '{module_name}' not found.")
    finally:
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def process_index_override():
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()
        cursor.execute(f"SELECT * FROM {configs.DB_FUNC_GET_INDEX_OVERRIDE}(%s);", [DOMAIN])
        data = cursor.fetchall()

        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        # Convert rows to a list of dictionaries
        result_dicts = [dict(zip(column_names, row)) for row in data]

        if len(result_dicts) == 0:
            return False
        
        logger.info(f"Index override identified.")
        logger.info(f"We will batch from {configs.DB_FIELD_INDEX_OVERRIDE_SOURCE_TS} to {configs.DB_FIELD_INDEX_OVERRIDE_TARGET_TS} "\
                    "in day increments of {configs.IDX_OVERRIDE_TIMESTEP_DAY_SIZE}")
        index_override_source_ts = result_dicts[0].get(configs.DB_FIELD_INDEX_OVERRIDE_SOURCE_TS) 
        index_override_target_ts = result_dicts[0].get(configs.DB_FIELD_INDEX_OVERRIDE_TARGET_TS) 
        index_override_batch_target_ts = index_override_source_ts

        while index_override_batch_target_ts <= index_override_target_ts:
            # add the IDX_OVERRIDE_TIMESTEP_DAY_SIZE # of days for batching
            index_override_batch_target_ts = index_override_source_ts + datetime.timedelta(days=int(configs.IDX_OVERRIDE_TIMESTEP_DAY_SIZE))

            logger.info(f"Processing batch: {index_override_source_ts} → {index_override_batch_target_ts}")

            # Fetch data for the batch range
            data = get_all(batch_start_ts=index_override_source_ts, batch_end_ts=index_override_batch_target_ts)
            process_business_logic(module_name=f"business_logic.{DOMAIN.lower()}", data=data)
            update_solr(arrow_table=data, solr_url=solr_url)

            # Move to the next batch (set new source timestamp as the last processed target)
            index_override_source_ts = index_override_batch_target_ts

        # Delete record from index_override table
        call_statement = f"CALL {configs.DB_FUNC_CLEAN_INDEX_OVERRIDE}(%s)"
        params = (DOMAIN,)
        cursor.execute(call_statement, params)
        conn.commit()

        return True

    except Exception as e:
        logger.error(f"Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")


if __name__ == "__main__":   
    parser = argparse.ArgumentParser(description=f"Index manager, that either 1.) listens to db events for updates, or 2.) does full load")
    parser.add_argument("-d", "--domain", help="Domain name i.e. asset, facility...", required=False, default=configs.DOMAIN_NAME_ASSET, type=str)
    parser.add_argument("-l", "--listener", help="Start listener", required=False, default=True, type=bool)
    parser.add_argument("-f", "--full", help="Full load", required=False, default=False, type=bool)
    args = parser.parse_args()

    if  os.getenv("DOMAIN"):
        DOMAIN = os.getenv("DOMAIN").upper().strip().replace("'", "")
    else:
        DOMAIN = args.domain.upper().upper().strip().replace("'", "")

    if DOMAIN == None:
        logger.error(f"Cannot location DOMAIN: {args.domain.upper()}")
        sys.exit(1)

    SOLR_COLLECTION = getattr(configs, f"SOLR_COLLECTION_{DOMAIN}")
    SOLR_URL = f"{configs.SOLR_URL}/{SOLR_COLLECTION}"
    DB_CHANNEL = getattr(configs, f"DB_CHANNEL_{DOMAIN}")
    DB_FUNC_GET_BY_ID = getattr(configs, f"DB_FUNC_GET_BY_ID_{DOMAIN}")
    DB_FUNC_GET = getattr(configs, f"DB_FUNC_GET_{DOMAIN}")
    IDX_BUFFER_SIZE = getattr(configs, f"IDX_BUFFER_SIZE_{DOMAIN}")
    IDX_BUFFER_DURATION = getattr(configs, f"IDX_BUFFER_DURATION_{DOMAIN}")
    IDX_FETCH_KEY = getattr(configs, f"IDX_FETCH_KEY_{DOMAIN}")
    DB_FUNC_CLEAN_INDEX_OVERRIDE = configs.DB_FUNC_CLEAN_INDEX_OVERRIDE

    logger.debug(f"IDX_BUFFER_SIZE: {IDX_BUFFER_SIZE}")
    logger.info(f"DOMAIN: {DOMAIN}")
    solr_url = f"{configs.SOLR_URL}/{getattr(configs, f"SOLR_COLLECTION_{DOMAIN}")}"
    logger.info (f"SOLR_URL: {solr_url}")

    if args.full:
        process_all(solr_url=solr_url)
    if args.listener:
        event_listener(solr_url=solr_url)

    
    
    
