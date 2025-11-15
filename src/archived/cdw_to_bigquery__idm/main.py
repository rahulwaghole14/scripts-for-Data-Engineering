"""
copy idm data to bigquery from cdw
"""
# pylint: disable=import-error
import json
import logging
import os
import base64
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq
from logger import logger

# destination table bigquery
# -- stage.idm.drupal__user_profiles


def clean_column_names(df):
    """
    clean column names
    """
    try:
        # Replace spaces with underscores and remove special characters
        df.columns = df.columns.str.replace(" ", "_", regex=True).str.replace(
            "?", "", regex=False
        )
    except Exception as err:
        logging.info("error in clean_column_names(): %s", err)

    return df


def convert_datetime_to_str(df):
    """
    convert dt to string
    """
    try:
        # Define the list of columns to convert
        datetime_columns = [
            "date_of_birth",
            "created_date",
            "last_modified",
            "verified_date",
            "mobile_verified_date",
            "year_of_birth",
            "record_load_dts_utc",
        ]

        for col in datetime_columns:
            if col in df.columns:
                # logging.info(f"Converting datetime column {col} to string")
                df[col] = df[col].astype(str)
    except Exception as err:
        logging.info("error in convert_datetime_to_str(): %s", err)

    return df


def load_config():
    """
    loading env vars
    """
    load_dotenv()

    encoded_cred = os.getenv("GOOGLE_CLOUD_CRED_BASE64")
    decoded_cred = base64.b64decode(encoded_cred).decode("utf-8")

    config = {
        "server": os.environ.get("DB_SERVER"),
        "port": os.environ.get("DB_PORT"),
        "database": os.environ.get("DB_DATABASE"),
        "username": os.environ.get("DB_USERNAME"),
        "password": os.environ.get("DB_PASSWORD"),
        "driver": os.environ.get("DB_DRIVER"),
        "google_cred": json.loads(decoded_cred),
        "project_id": "hexa-data-report-etl-prod",
        "destination_table": "cdw_stage.idm__user_profiles",
    }
    return config


if __name__ == "__main__":
    logger("idm")
    try:
        config = load_config()
        credentials = Credentials.from_service_account_info(
            config["google_cred"]
        )

        conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver={config['driver']}&TrustServerCertificate=yes"
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY = "SELECT * FROM stage.idm.drupal__user_profiles"

        CHUNK_SIZE = 50000
        FIRST_CHUNK = True

        date_columns = []
        chunk_number = 0  # Add a chunk counter for debugging

        # do first table
        for chunk in pd.read_sql(QUERY, engine, chunksize=CHUNK_SIZE):
            chunk_number += 1  # Increment the chunk counter
            logging.info(f"Received chunk of size {len(chunk)}")

            # Clean the column names
            chunk = clean_column_names(chunk)

            # Convert native datetime objects to string
            chunk = convert_datetime_to_str(chunk)

            IF_EXISTS = "replace" if FIRST_CHUNK else "append"
            to_gbq(
                chunk,
                config["destination_table"],
                project_id=config["project_id"],
                if_exists=IF_EXISTS,
                credentials=credentials,
            )
            FIRST_CHUNK = False

        logging.info("Data migration completed successfully.")

    except Exception as error:
        logging.error("An error occurred: %s", error)
