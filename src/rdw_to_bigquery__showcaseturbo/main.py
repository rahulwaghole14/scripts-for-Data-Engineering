# pylint: disable=import-error
import json
import logging
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from logger import logger



def clean_column_names(df):
    # Replace spaces with underscores and remove special characters
    df.columns = df.columns.str.replace(" ", "_", regex=True).str.replace("?", "", regex=True)
    return df

def load_config():
    load_dotenv()
    config = {
        "server": os.environ.get("DB_SERVER_RDW"),
        "database": os.environ.get("DB_DATABASE_RDW"),
        "driver": os.environ.get("DB_DRIVER"),
        "google_cred": json.loads(os.getenv("GOOGLE_CLOUD_CRED")),
        "project_id": 'hexa-data-report-etl-prod',
        "destination_table": 'adw_stage.showcaseturbo__adrevenue',
    }
    return config


if __name__ == "__main__":
    logger('showcaseturbo')

    try:
        config = load_config()

        conn_str = f"mssql+pyodbc://{config['server']}/{config['database']}?driver={config['driver']}&Trusted_Connection=yes&TrustServerCertificate=yes"

        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

        QUERY = "SELECT * FROM [dbo].[ShowcaseTurbo]"

        # Output CSV file path
        csv_file_path = 'showcase_turbo_datanew.csv'

        # Download all data to a CSV file
        pd.read_sql_query(QUERY, engine).to_csv(csv_file_path, index=False, encoding='utf-8')

        logging.info("Data downloaded to %s.", csv_file_path)

    except Exception as error:
        logging.error("An error occurred: %s", error)

        logging.info("Data downloaded successfully.")
