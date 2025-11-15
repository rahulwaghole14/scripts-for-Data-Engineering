# pylint: disable=import-error
import json
import logging
import os
import re
import pandas as pd
import sqlalchemy
import datetime
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq
from logger import logger

logging.basicConfig(level=logging.INFO)
logging.getLogger('pandas_gbq').setLevel(logging.INFO)

def load_config():
    load_dotenv()
    config = {
        "server": os.environ.get("DB_SERVER_RDW"),
        "database": os.environ.get("DB_DATABASE_RDW"),
        "driver": os.environ.get("DB_DRIVER"),
        "google_cred": json.loads(os.getenv("GOOGLE_CLOUD_CRED")),
        "project_id": 'hexa-data-report-etl-prod',
        "destination_table": 'adw_stage.naviga__adrevenue',
    }
    return config

def clean_column_names(df):
    # Replace spaces with underscores and remove special characters
    df.columns = df.columns.str.replace(" ", "_", regex=True).str.replace("?", "", regex=True)

    # Convert column names to lowercase
    df.columns = df.columns.str.lower()

    return df


if __name__ == "__main__":
    logger('showcaseturbo')

    try:
        config = load_config()

        conn_str = f"mssql+pyodbc://{config['server']}/{config['database']}?driver={config['driver']}&Trusted_Connection=yes&TrustServerCertificate=yes"

        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY = "SELECT * FROM [dbo].[NavigaTest]"
        QUERY2 = "SELECT * FROM [dbo].[NavigaAccountMapping]"

        # Output CSV file path
        csv_file_path = 'naviga_data.csv'
        csv_file_path2 = 'naviga_accountmapping.csv'

        # Download all data to a CSV file
        pd.read_sql_query(QUERY, engine).to_csv(csv_file_path, index=False, encoding='utf-8')

        # Clean column names in the CSV file
        cleaned_df = clean_column_names(pd.read_csv(csv_file_path, encoding='utf-8'))

        # Save the cleaned data back to the CSV file
        cleaned_df.to_csv(csv_file_path, index=False, encoding='utf-8')

        logging.info("Data downloaded to %s.", csv_file_path)

        # Download all data to a CSV file 2
        pd.read_sql_query(QUERY2, engine).to_csv(csv_file_path2, index=False, encoding='utf-8')

        # Clean column names in the CSV file
        cleaned_df2 = clean_column_names(pd.read_csv(csv_file_path2, encoding='utf-8'))

        # Save the cleaned data back to the CSV file
        cleaned_df2.to_csv(csv_file_path2, index=False, encoding='utf-8')

        logging.info("Data downloaded to %s.", csv_file_path2)

    except Exception as error:
        logging.error("An error occurred: %s", error)

        logging.info("Data downloaded successfully.")

# -- historic data for adbook (one off migration)
# SELECT count(*) FROM [FFXDW].[dbo].[AdBookNavigaHistoric]; -- 114,297
# -- historic data for genera (one off migration)
# SELECT count(*) FROM [FFXDW].[dbo].[GeneraNavigaHistoric]; -- 868,670
# -- historic data for showcaseturbo (one off migration)
# SELECT count(*) FROM [FFXDW].[dbo].[ShowcaseTurbo]; -- 789
# -- naviga new data table rdw (one off migration and switch to bigquery?)
# SELECT count(*) FROM [FFXDW].[dbo].[NavigaTest]; -- 133,611
# -- naviga accounts to genera accounts mapping table (one off migration)
# SELECT count(*) FROM [FFXDW].[dbo].[NavigaAccountMapping]; -- 5,533
