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

logger, config = bootstrap()
configs = config.get_configs()

def setup_connection():
    db_config = {
    'dbname': configs.DATABASE_NAME,
    'user': config.get_secret('DATABASE_USER'),
    'password': config.get_secret('DATABASE_PASSWORD'),
    'host': configs.DATABASE_HOST,
    'port': configs.DATABASE_PORT
}

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    return conn, cursor

def get_all():
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()
        cursor.callproc(f"{getattr(configs, f"DB_PROC_GET_{DOMAIN}")}")
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

    return arrow_table

def get_by_id(json_data):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        conn, cursor = setup_connection()
        cursor.execute(f"SELECT * FROM {getattr(configs, f"DB_PROC_GET_BY_ID_{DOMAIN}")}(%s);", [json_data])
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
    return arrow_table

def apply_business_logic(arrow_table):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    #Not used yet.
    df = arrow_table.to_pandas()
    # df.loc[df['category'] == 'Real Estate', 'value'] *= 1.10
    return pa.Table.from_pandas(df)

def update_solr(arrow_table, solr_url):
    logger.debug(f"BEGIN {inspect.currentframe().f_code.co_name}")
    try:
        solr = pysolr.Solr(solr_url)

        if arrow_table == None:
            logger.warning(f"No records passed to {inspect.currentframe().f_code.co_name}")
            return
        # Convert Arrow Table back to DataFrame and then to dict for SOLR
        solr_data = arrow_table.to_pandas().to_dict(orient='records')
        solr.add(solr_data)
        logger.info(f"Successfully updated {len(solr_data)} documents in SOLR.")
    except Exception as e:
        logger.error(f"Error in {inspect.currentframe().f_code.co_name}: {e}")
    

def process_all(solr_url):
    data = get_all()
    # processed_data = apply_business_logic(data)
    update_solr(arrow_table=data, solr_url=solr_url)

def event_listener(solr_url):
    try:
        listener_conn, listener_cursor = setup_connection()
        listener_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        reader_conn, reader_cursor = setup_connection()

        listener_cursor.execute(f"LISTEN {getattr(configs, f"DB_CHANNEL_{DOMAIN}")};")
        logger.info(f"Listening for {getattr(configs, f"DB_CHANNEL_{DOMAIN}")} events...")

        notify_buffer = []
        last_executed_time = time.time()

        while True:
            listener_conn.poll()
            while listener_conn.notifies:
                notify = listener_conn.notifies.pop(0)
                logger.debug(f"🔔 {getattr(configs, f"DB_CHANNEL_{DOMAIN}")} Change Detected: {notify.payload}")
                notify_buffer.append(notify.payload)

            if len(notify_buffer) > int(getattr(configs, f"IDX_BUFFER_SIZE_{DOMAIN}")) or (time.time() - last_executed_time >= int(getattr(configs, f"IDX_BUFFER_DURATION_{DOMAIN}"))):
                if notify_buffer:

                    json_data = json.dumps({f"{getattr(configs, f"IDX_FETCH_KEY_{DOMAIN}")}": notify_buffer}) 
                    data = get_by_id(json_data=json_data)
                    # processed_data = apply_business_logic(data)
                    update_solr(arrow_table=data, solr_url=solr_url)

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

if __name__ == "__main__":   
    parser = argparse.ArgumentParser(description=f"Index manager, that either 1.) listens to db events for updates, or 2.) does full load")
    parser.add_argument("-d", "--domain", help="Domain name i.e. asset, facility...", required=True, default=configs.DOMAIN_NAME_ASSET, type=str)
    parser.add_argument("-l", "--listener", help="Start listener", required=False, default=True, type=bool)
    parser.add_argument("-f", "--full", help="Full load", required=False, default=False, type=bool)
    args = parser.parse_args()

    args.domain.upper()

    DOMAIN = getattr(configs, f"DOMAIN_NAME_{args.domain.upper()}", None)

    if DOMAIN == None:
        logger.error(f"Cannot location DOMAIN: {args.domain.upper()}")
        sys.exit(1)

    solr_url = f"{configs.SOLR_URL}/{getattr(configs, f"SOLR_COLLECTION_{DOMAIN}")}"
    logger.info (f'SOLR_URL: {solr_url}')

    if args.full:
        process_all(solr_url=solr_url)
    if args.listener:
        event_listener(solr_url=solr_url)

    
    
    
