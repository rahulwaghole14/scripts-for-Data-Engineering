# pylint: disable=all
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

# -- stage.acm.print_blacklist_final
# -- stage.acm.print_data_blacklist
# -- stage.acm.print_data_generic
# -- stage.acm.print_subbenefit_final
# -- stage.acm.print_generic_final


def clean_column_names(df):
    # Replace spaces with underscores and remove special characters
    df.columns = df.columns.str.replace(" ", "_", regex=True).str.replace(
        "?", "", regex=False
    )
    return df


def load_config():
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
        "destination_table_print_blacklist_final": "cdw_stage.acm__print_blacklist_final",
        "destination_table_print_data_blacklist": "cdw_stage.acm__print_data_blacklist",
        "destination_table_print_data_generic": "cdw_stage.acm__print_data_generic",
        "destination_table_print_subbenefit_final": "cdw_stage.acm__print_subbenefit_final",
        "destination_table_print_generic_final": "cdw_stage.acm__print_generic_final",
        "destination_table_generic_print_subs": "cdw_stage.acm__generic_print_subs",
        "destination_table_generic_magazine_print_subs": "cdw_stage.acm__generic_magazine_print_subs",
    }
    return config


if __name__ == "__main__":
    logger("presspatron")
    try:
        config = load_config()
        credentials = Credentials.from_service_account_info(
            config["google_cred"]
        )

        conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver={config['driver']}&TrustServerCertificate=yes"
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY_PRINT_BLACKLIST_FINAL = (
            "SELECT * FROM stage.acm.print_blacklist_final"
        )
        QUERY_PRINT_DATA_BLACKLIST = (
            "SELECT * FROM stage.acm.print_data_blacklist"
        )
        QUERY_PRINT_DATA_GENERIC = "SELECT * FROM stage.acm.print_data_generic"
        QUERY_PRINT_SUBBENEFIT_FINAL = (
            "SELECT * FROM stage.acm.print_subbenefit_final"
        )
        QUERY_PRINT_GENERIC_FINAL = (
            "SELECT * FROM stage.acm.print_generic_final"
        )
        QUERY_PRINT_GENERIC_PRINT_SUBS = (
            "SELECT * FROM stage.acm.generic_print_subs"
        )
        QUERY_PRINT_GENERIC_MAGAZINE_PRINT_SUBS = (
            "SELECT * FROM stage.acm.generic_magazine_print_subs"
        )

        CHUNK_SIZE = 50000
        FIRST_CHUNK = True

        for chunk in pd.read_sql(
            QUERY_PRINT_GENERIC_PRINT_SUBS, engine, chunksize=CHUNK_SIZE
        ):
            logging.info(f"Received chunk of size {len(chunk)}")

            # Clean the column names
            chunk = clean_column_names(chunk)

            IF_EXISTS = "replace" if FIRST_CHUNK else "append"
            to_gbq(
                chunk,
                config["destination_table_generic_print_subs"],
                project_id=config["project_id"],
                if_exists=IF_EXISTS,
                credentials=credentials,
            )
            FIRST_CHUNK = False

        for chunk in pd.read_sql(
            QUERY_PRINT_GENERIC_MAGAZINE_PRINT_SUBS,
            engine,
            chunksize=CHUNK_SIZE,
        ):
            logging.info(f"Received chunk of size {len(chunk)}")

            # Clean the column names
            chunk = clean_column_names(chunk)

            IF_EXISTS = "replace" if FIRST_CHUNK else "append"
            to_gbq(
                chunk,
                config["destination_table_generic_magazine_print_subs"],
                project_id=config["project_id"],
                if_exists=IF_EXISTS,
                credentials=credentials,
            )
            FIRST_CHUNK = False
        # do first table
        # for chunk in pd.read_sql(QUERY_PRINT_BLACKLIST_FINAL, engine, chunksize=CHUNK_SIZE):
        #     logging.info(f"Received chunk of size {len(chunk)}")

        #     # Clean the column names
        #     chunk = clean_column_names(chunk)

        #     IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
        #     to_gbq(chunk, config['destination_table_print_blacklist_final'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
        #     FIRST_CHUNK = False

        # # do first table
        # for chunk in pd.read_sql(QUERY_PRINT_DATA_BLACKLIST, engine, chunksize=CHUNK_SIZE):
        #     logging.info(f"Received chunk of size {len(chunk)}")

        #     # Clean the column names
        #     chunk = clean_column_names(chunk)

        #     IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
        #     to_gbq(chunk, config['destination_table_print_data_blacklist'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
        #     FIRST_CHUNK = False

        # # do first table
        # for chunk in pd.read_sql(QUERY_PRINT_DATA_GENERIC, engine, chunksize=CHUNK_SIZE):
        #     logging.info(f"Received chunk of size {len(chunk)}")

        #     # Clean the column names
        #     chunk = clean_column_names(chunk)

        #     IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
        #     to_gbq(chunk, config['destination_table_print_data_generic'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
        #     FIRST_CHUNK = False

        # # do first table
        # for chunk in pd.read_sql(QUERY_PRINT_SUBBENEFIT_FINAL, engine, chunksize=CHUNK_SIZE):
        #     logging.info(f"Received chunk of size {len(chunk)}")

        #     # Clean the column names
        #     chunk = clean_column_names(chunk)

        #     IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
        #     to_gbq(chunk, config['destination_table_print_subbenefit_final'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
        #     FIRST_CHUNK = False

        # # do first table
        # for chunk in pd.read_sql(QUERY_PRINT_GENERIC_FINAL, engine, chunksize=CHUNK_SIZE):
        #     logging.info(f"Received chunk of size {len(chunk)}")

        #     # Clean the column names
        #     chunk = clean_column_names(chunk)

        #     IF_EXISTS = 'replace' if FIRST_CHUNK else 'append'
        #     to_gbq(chunk, config['destination_table_print_generic_final'], project_id=config['project_id'], if_exists=IF_EXISTS, credentials=credentials)
        #     FIRST_CHUNK = False

        logging.info("Data migration completed successfully.")

    except Exception as error:
        logging.error("An error occurred: %s", error)
