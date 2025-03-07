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
from util import utilities
import psycopg2
import pysolr
import json
import argparse
import inspect
import pyarrow as pa
import pandas as pd
import datetime
import importlib
import numpy as np
import concurrent.futures

def get_all(batch_start_ts=None, batch_end_ts=None):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = utilities.setup_connection(config=config)

        if batch_start_ts == None and batch_end_ts == None:
            cursor.execute(f"SELECT * FROM {DB_FUNC_GET}();", [])
        else:
            cursor.execute(f"SELECT * FROM {DB_FUNC_GET}(%s, %s, %s);", [None, batch_start_ts, batch_end_ts])
            
        data = cursor.fetchall()
        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        
        # pyarrow does not support jsonb, so we have to convert to string on those fields.
        df = pd.DataFrame(
            [[utilities.convert_jsonb(value) for value in row] for row in data], 
            columns=column_names
        )

        # Convert DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)

    except Exception as e:
        logger.exception(f"❌Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

    return arrow_table

def get_by_id(notify_buffer):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        json_data = json.dumps({f"{IDX_FETCH_KEY}": notify_buffer}) 

        conn, cursor = utilities.setup_connection(config=config)
        cursor.execute(f"SELECT * FROM {DB_FUNC_GET_BY_ID}(%s, %s);", [json_data, None])
        data = cursor.fetchall()

        logger.debug(f"{len(data)} records received from {DB_FUNC_GET_BY_ID}")

        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]

        # pyarrow does not support jsonb, so we have to convert to string on those fields.
        df = pd.DataFrame(
            [[utilities.convert_jsonb(value) for value in row] for row in data], 
            columns=column_names
        )

        # Convert DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)
    except Exception as e:
        logger.exception(f"❌Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")
    return arrow_table

def clean_event_notification_by_id(notify_buffer, channel_name):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        json_data = json.dumps({f"{IDX_EVENT_FETCH_KEY}": notify_buffer}) 

        conn, cursor = utilities.setup_connection(config=config)
        cursor.execute(f"SELECT * FROM {configs.DB_FUNC_CLEAN_EVENT_NOTIFICATION_BUFFER}(%s, %s);", [json_data, channel_name])
        conn.commit()
    except Exception as e:
        logger.exception(f"❌Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def update_solr(arrow_table, solr_url):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        solr = pysolr.Solr(
            solr_url,
            always_commit=True,
            timeout=10,
            auth=(config.get_secret("SOLR_USER"), config.get_secret("SOLR_PASSWORD")) 
        )

        if arrow_table == None:
            logger.warning(f"No records passed to {inspect.currentframe().f_code.co_name}")
            return
        # Convert Arrow Table back to DataFrame and then to dict for SOLR
        solr_data = arrow_table.to_pandas().to_dict(orient="records")

        # Format records (timestamptz) to be compatible with Solr
        for record in solr_data:
            utilities.convert_timestamptz_to_date(record)

            # Convert all NumPy arrays and JSONB lists to Python lists
            for key, value in record.items():
                if isinstance(value, np.ndarray):  # NumPy arrays
                    record[key] = value.tolist()
                elif isinstance(value, str):  # Check if it's still a JSON string
                    try:
                        json_value = json.loads(value)
                        if isinstance(json_value, list):  # Convert only if it's a list
                            record[key] = json_value
                    except json.JSONDecodeError:
                        pass  # Ignore if it fails

        solr.add(solr_data)
        logger.info(f"{len(solr_data)} documents successfully updated in SOLR.")
    except Exception as e:
        logger.exception(f"❌Error in {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")
    
def process_all(solr_url):
    if not process_index_override():
        arrow_table = get_all()
        process_business_logic(module_name=f"business_logic.{DOMAIN}", data=arrow_table)
        update_solr(arrow_table=arrow_table, solr_url=solr_url)

def event_listener(solr_url):
    retry_delay = int(configs.IDX_BUFFER_RETRY_SECONDS)

    while True:
        try:
            logger.info("🔄 Establishing connection to PostgreSQL...")
            listener_conn, listener_cursor = utilities.setup_connection(config=config)
            listener_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            listener_cursor.execute(f"LISTEN {DB_CHANNEL};")
            logger.info(f"✅ Listening for {DB_CHANNEL} events...")

            notify_buffer = []
            last_executed_time = time.time()

            # Recover buffered events from the DB
            logger.info(f"Recovering buffered events before enabling listener")
            listener_cursor.execute(f"SELECT * FROM {configs.DB_FUNC_GET_EVENT_NOTIFICATION_BUFFER}(%s);", [DB_CHANNEL])
            buffered_event_data = listener_cursor.fetchall()

            logger.debug(f"{len(buffered_event_data)} records received from {configs.DB_FUNC_GET_EVENT_NOTIFICATION_BUFFER}")

            for event in buffered_event_data:
                notification_id, channel, payload, *extra_columns = event
                logger.debug(f"📥 Buffered event - ID: {notification_id}, Channel: {channel}, Payload: {payload}")
                notify_buffer.append(payload)

            # Main event listening loop
            while True:
                listener_conn.poll()
                while listener_conn.notifies:
                    notify = listener_conn.notifies.pop(0)
                    logger.debug(f"🔔 {DB_CHANNEL} Change Detected: {notify.payload} 🔔")
                    notify_buffer.append(notify.payload)

                # Process buffered events periodically or when buffer size exceeds limit
                if len(notify_buffer) > int(IDX_BUFFER_SIZE) or (time.time() - last_executed_time >= int(IDX_BUFFER_DURATION)):
                    if notify_buffer:
                        data = get_by_id(notify_buffer=notify_buffer)
                        process_business_logic(module_name=f"business_logic.{DOMAIN}", data=data)
                        update_solr(arrow_table=data, solr_url=solr_url)

                        # Remove processed events from the buffer
                        clean_event_notification_by_id(notify_buffer=notify_buffer, channel_name=DB_CHANNEL)

                        notify_buffer.clear()
                        last_executed_time = time.time()

        except psycopg2.OperationalError as e:
            logger.error(f"❌ Database connection lost: {e}")
            logger.info(f"⏳ Retrying connection in {retry_delay} seconds...")
            time.sleep(retry_delay)  # Wait before retrying

        except Exception as e:
            logger.exception(f"❌ Unexpected error in {inspect.currentframe().f_code.co_name}: {e}")
            logger.info(f"⏳ Retrying connection in {retry_delay} seconds...")
            time.sleep(retry_delay)  # Wait before retrying

        finally:
            try:
                if listener_cursor:
                    listener_cursor.close()
                if listener_conn:
                    listener_conn.close()
                logger.info("🔌 Closed database connection. Reconnecting...")
            except Exception as cleanup_error:
                logger.error(f"⚠️ Error while closing DB connection: {cleanup_error}")

def process_business_logic(module_name, data):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        # Dynamically import the module
        module = importlib.import_module(module_name.lower())
        
        # Check if the module has the expected function
        if hasattr(module, "process"):
            func = getattr(module, "process")
            func(data)  # Execute the function
        else:
            logger.warning(f"Module '{module_name}' does not contain a 'process' function.")
    except ModuleNotFoundError:
        logger.warning(f"Module '{module_name}' not found.")
    finally:
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def process_index_override():
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = utilities.setup_connection(config=config)
        cursor.execute(f"SELECT * FROM {configs.DB_FUNC_GET_INDEX_OVERRIDE}(%s);", [DOMAIN])
        data = cursor.fetchall()

        # Dynamically get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        # Convert rows to a list of dictionaries
        result_dicts = [dict(zip(column_names, row)) for row in data]

        if len(result_dicts) == 0:
            return False
        
        # This feature is to suppliment the "full" load, where a single pull is too much.  It will read the
        # index_override table, which has a domain, a source timestamp and target timestamp.  It will batch
        # the load into day increments in the IDX_OVERRIDE_TIMESTEP_DAY_SIZE configuration.  The default is 7.
        # So this means it will fetch 7 days of data at a time until we reach the target timestamp.
        # It will also use concurrent threadpool to run multiple batches at the same time.
        logger.info(f"🔄 Index override identified.")
        logger.info(f"🔄 We will batch from {configs.DB_FIELD_INDEX_OVERRIDE_SOURCE_TS} to {configs.DB_FIELD_INDEX_OVERRIDE_TARGET_TS} "\
                    "in day increments of {configs.IDX_OVERRIDE_TIMESTEP_DAY_SIZE}")
        index_override_source_ts = result_dicts[0].get(configs.DB_FIELD_INDEX_OVERRIDE_SOURCE_TS) 
        index_override_target_ts = result_dicts[0].get(configs.DB_FIELD_INDEX_OVERRIDE_TARGET_TS) 
        
        index_override_batch_source_ts = index_override_source_ts
        index_override_batch_target_ts = index_override_source_ts

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(configs.IDX_OVERRIDE_CONCURRENT_THREAD_COUNT)) as executor:
            while index_override_batch_target_ts <= index_override_target_ts:
                # Add the IDX_OVERRIDE_TIMESTEP_DAY_SIZE # of days for batching
                index_override_batch_target_ts = index_override_batch_source_ts + datetime.timedelta(days=int(configs.IDX_OVERRIDE_TIMESTEP_DAY_SIZE))

                # params sent to process_batch
                batch_params = {
                    "batch_start_ts": index_override_batch_source_ts,
                    "batch_end_ts": index_override_batch_target_ts,
                    "domain": DOMAIN,
                    "solr_url": solr_url,
                }
                # Submit batch processing task to the thread pool
                future = executor.submit(process_batch, **batch_params)
                futures.append(future)

                # Move to the next batch (set new source timestamp as the last processed target)
                index_override_batch_source_ts = index_override_batch_target_ts

            # Wait for all threads to finish and collect results
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if not result:
                        logger.warning("⚠️ Some batch processing tasks failed.")
                except Exception as e:
                    logger.exception(f"❌ Error processing batch: {e}")

        logger.info("🎉 All batch processing tasks are complete.")
        # Archive record from index_override table
        call_statement = f"CALL {configs.DB_FUNC_CLEAN_INDEX_OVERRIDE}(%s)"
        params = (DOMAIN,)
        cursor.execute(call_statement, params)
        conn.commit()

        return True

    except Exception as e:
        logger.exception(f"❌Error {inspect.currentframe().f_code.co_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.debug(f"END {inspect.currentframe().f_code.co_name}")

def process_batch(**kwargs):
    """Function to be executed in a thread: Fetch, process, and update."""

    batch_start_ts = kwargs.get("batch_start_ts")
    batch_end_ts = kwargs.get("batch_end_ts")
    domain = kwargs.get("domain")
    solr_url = kwargs.get("solr_url")

    logger.info(f"🔄 Processing batch: {batch_start_ts} → {batch_end_ts}")

    data = get_all(batch_start_ts=batch_start_ts, batch_end_ts=batch_end_ts)
    
    if not data:
        return True
    
    process_business_logic(module_name=f"business_logic.{domain}", data=data)
    update_solr(arrow_table=data, solr_url=solr_url)

    logger.info(f"✅ Batch {batch_start_ts} → {batch_end_ts} processed successfully.")
    return True


if __name__ == "__main__": 
    logger, config = bootstrap()
    configs = config.get_configs()

    parser = argparse.ArgumentParser(description=f"Index manager, that either 1.) listens to db events for updates, or 2.) does full load")
    parser.add_argument("-d", "--domain", help="Domain name i.e. account, facility, asset,", required=False, type=str)
    parser.add_argument("-l", "--listener", help="Start listener", required=False, action="store_true")
    parser.add_argument("-f", "--full", help="Full load", required=False, action="store_true")
    args = parser.parse_args()

    if args.domain is not None:
        DOMAIN = args.domain.upper().upper().strip().replace("'", "")
    elif os.getenv("DOMAIN"):
        DOMAIN = os.getenv("DOMAIN").upper().strip().replace("'", "")

    if DOMAIN == None:
        logger.exception(f"❌Cannot location DOMAIN: {args.domain.upper()}")
        sys.exit(1)

    SOLR_COLLECTION = getattr(configs, f"SOLR_COLLECTION_{DOMAIN}")
    SOLR_URL = f"{configs.SOLR_URL}/{SOLR_COLLECTION}"
    DB_CHANNEL = getattr(configs, f"DB_CHANNEL_{DOMAIN}")
    DB_FUNC_GET_BY_ID = getattr(configs, f"DB_FUNC_GET_BY_ID_{DOMAIN}")
    DB_FUNC_GET = getattr(configs, f"DB_FUNC_GET_{DOMAIN}")
    IDX_BUFFER_SIZE = getattr(configs, f"IDX_BUFFER_SIZE_{DOMAIN}")
    IDX_BUFFER_DURATION = getattr(configs, f"IDX_BUFFER_DURATION_{DOMAIN}")
    IDX_FETCH_KEY = getattr(configs, f"IDX_FETCH_KEY_{DOMAIN}")
    IDX_EVENT_FETCH_KEY = configs.IDX_EVENT_FETCH_KEY

    logger.info(f"DOMAIN: {DOMAIN}")
    logger.debug(f"SOLR_COLLECTION: {SOLR_COLLECTION}")
    logger.debug(f"SOLR_URL: {IDX_EVENT_FETCH_KEY}")
    logger.debug(f"DB_CHANNEL: {DB_CHANNEL}")
    logger.debug(f"DB_FUNC_GET_BY_ID: {DB_FUNC_GET_BY_ID}")
    logger.debug(f"DB_FUNC_GET: {DB_FUNC_GET}")
    logger.debug(f"IDX_BUFFER_SIZE: {IDX_BUFFER_SIZE}")
    logger.debug(f"IDX_BUFFER_DURATION: {IDX_BUFFER_DURATION}")
    logger.debug(f"IDX_EVENT_FETCH_KEY: {IDX_EVENT_FETCH_KEY}")
    logger.debug(f"IDX_FETCH_KEY: {IDX_FETCH_KEY}")
    
    solr_url = f"{configs.SOLR_URL}/{getattr(configs, f"SOLR_COLLECTION_{DOMAIN}")}"
    logger.info (f"SOLR_URL: {solr_url}")

    if args.full:
        process_all(solr_url=solr_url)
    if args.listener:
        event_listener(solr_url=solr_url)

    
    
    
