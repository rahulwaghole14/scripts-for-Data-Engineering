# pylint: disable=import-error
import json
import logging
import os
import base64
import pandas as pd
import sqlalchemy
import datetime
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq
from logger import logger

def load_config():
    load_dotenv()

    encoded_cred = os.getenv("GOOGLE_CLOUD_CRED_BASE64")
    decoded_cred = base64.b64decode(encoded_cred).decode('utf-8')

    config = {
        "server": os.environ.get("DB_SERVER"),
        "port": os.environ.get("DB_PORT"),
        "database": os.environ.get("DB_DATABASE"),
        "username": os.environ.get("DB_USERNAME"),
        "password": os.environ.get("DB_PASSWORD"),
        "driver": os.environ.get("DB_DRIVER"),
        "google_cred": json.loads(decoded_cred),
        "project_id": 'hexa-data-report-etl-prod',
        "destination_table_customer": 'cdw_stage.matrix__customer',
        "destination_table_subscription": 'cdw_stage.matrix__subscription',
        "destination_table_subscriber": 'cdw_stage.matrix__subscriber',
        "destination_table_subord_cancel": 'cdw_stage.matrix__subord_cancel'
    }
    return config

if __name__ == "__main__":
    logger('matrix')
    try:
        config = load_config()
        credentials = Credentials.from_service_account_info(config['google_cred'])

        conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver={config['driver']}&TrustServerCertificate=yes"
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY_CUSTOMER = "SELECT * FROM stage.matrix.customer"
        QUERY_SUBSCRIPTION = "SELECT * FROM stage.matrix.subscription"
        QUERY_SUBSCRIBER = "SELECT * FROM stage.matrix.subscriber"
        QUERY_SUBORD_CANCEL = "SELECT * FROM stage.matrix.subord_cancel"

        CHUNK_SIZE = 50000
        FIRST_CHUNK = True

        # do first table
        for chunk in pd.read_sql(QUERY_CUSTOMER, engine, chunksize=CHUNK_SIZE):
            logging.info(f"Received chunk of size {len(chunk)}")

            IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
            to_gbq(chunk, config['destination_table_customer'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
            FIRST_CHUNK = False

        # do first table
        # List of columns that are known to contain datetime.date objects based on your logging
        date_columns = ['sord_entry_date', 'sord_startdate', 'sord_stopdate', 'exp_end_date', 'last_invoiced', 'PaidThruDate', 'record_load_dts']

        for chunk in pd.read_sql(QUERY_SUBSCRIPTION, engine, chunksize=CHUNK_SIZE):
            logging.info(f"Received chunk of size {len(chunk)}")

            # Automatically convert known problematic columns to string or another compatible type
            for date_col in date_columns:
                if date_col in chunk.columns:
                    chunk[date_col] = chunk[date_col].astype(str)

            IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
            to_gbq(chunk, config['destination_table_subscription'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
            FIRST_CHUNK = False

        # do first table
        for chunk in pd.read_sql(QUERY_SUBSCRIBER, engine, chunksize=CHUNK_SIZE):
            logging.info(f"Received chunk of size {len(chunk)}")

            IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
            to_gbq(chunk, config['destination_table_subscriber'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
            FIRST_CHUNK = False

        # do first table
        date_columns = ['canx_date', 'request_date', 'record_load_dts']

        for chunk in pd.read_sql(QUERY_SUBORD_CANCEL, engine, chunksize=CHUNK_SIZE):
            logging.info(f"Received chunk of size {len(chunk)}")

            # Automatically convert known problematic columns to string or another compatible type
            for date_col in date_columns:
                if date_col in chunk.columns:
                    chunk[date_col] = chunk[date_col].astype(str)

            IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
            to_gbq(chunk, config['destination_table_subord_cancel'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
            FIRST_CHUNK = False


        logging.info("Data migration completed successfully.")

    except Exception as error:
        logging.error("An error occurred: %s", error)
