# pylint: disable-all
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

def clean_column_names(df):
    # Replace spaces with underscores and remove special characters
    df.columns = df.columns.str.replace(" ", "_", regex=True).str.replace("?", "", regex=False)
    return df

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
        "destination_table": 'cdw_stage.presspatron__braze_user_profiles'
    }
    return config

if __name__ == "__main__":
    try:
        config = load_config()
        credentials = Credentials.from_service_account_info(config['google_cred'])

        conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver={config['driver']}&TrustServerCertificate=yes"
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY = "SELECT * FROM stage.presspatron.braze_user_profiles"

        CHUNK_SIZE = 50000
        FIRST_CHUNK = True

        # do first table
        for chunk in pd.read_sql(QUERY, engine, chunksize=CHUNK_SIZE):
            logging.info(f"Received chunk of size {len(chunk)}")

            # Clean the column names
            chunk = clean_column_names(chunk)

            IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
            to_gbq(chunk, config['destination_table'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
            FIRST_CHUNK = False

        logging.info("Data migration completed successfully.")

    except Exception as error:
        logging.error("An error occurred: %s", error)
