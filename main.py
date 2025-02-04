import sys
import os
from bootstrap import bootstrap
import psycopg2
import pysolr
import pyarrow as pa
import pandas as pd

def fetch_assets_from_postgresql():
    db_config = {
        'dbname': configs.DATABASE_NAME,
        'user': config.get_secret('DATABASE_USER'),
        'password': config.get_secret('DATABASE_PASSWORD'),
        'host': configs.DATABASE_HOST,
        'port': configs.DATABASE_PORT
    }

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.callproc('get_assets')
    assets = cursor.fetchall()

    cursor.close()
    conn.close()

    # Using pyarrow to convert fetched data to Arrow Table
    arrow_table = pa.Table.from_pandas(pd.DataFrame(assets, columns=['asset_id', 'sys_id', 'fac_code']))
    return arrow_table

def apply_business_logic(arrow_table):
    df = arrow_table.to_pandas()
    df.loc[df['category'] == 'Real Estate', 'value'] *= 1.10
    return pa.Table.from_pandas(df)

def update_solr_collection(arrow_table, solr_url):
    solr = pysolr.Solr(solr_url)

    # Convert Arrow Table back to DataFrame and then to dict for SOLR
    solr_data = arrow_table.to_pandas().to_dict(orient='records')
    solr.add(solr_data)
    print(f"Successfully updated {len(solr_data)} assets in SOLR.")

def main():
    arrow_table = fetch_assets_from_postgresql()
    processed_table = apply_business_logic(arrow_table)
    update_solr_collection(processed_table)

if __name__ == "__main__":
    logger, config = bootstrap()
    configs = config.get_configs()

    solr_url = f"{configs.SOLR_URL}/{configs.SOLR_COLLECTION_ASSET}"
    logger.info (f'SOLR_URL: {SOLR_URL}')
    # logger.debug(configs.DATABASE_NAME)
    # configs.as_dict()

    logger.info("Loaded config")
    fetch_assets_from_postgresql()