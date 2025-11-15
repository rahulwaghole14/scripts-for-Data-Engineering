import os
import sys
import json
from datetime import datetime, timedelta
from decimal import Decimal
import pyodbc
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account
from .validator.model_generator import (
    fetch_and_create_dynamic_model,
    validate_data_with_pydantic,
)
import logging

from common.aws.aws_secret import get_secret

# Configure logging
logging.basicConfig(level=logging.INFO)


secret_value = get_secret("datateam_CDW_and_Matrix_Database")
matrix_secrets = json.loads(secret_value)
sql_server = matrix_secrets.get("MATRIX_DB_SERVER_IP_NEW")
sql_database = matrix_secrets.get("MATRIX_DB_DATABASE")
sql_username = matrix_secrets.get("MATRIX_DB_USERNAME")
sql_password = matrix_secrets.get("MATRIX_DB_PASSWORD")
sql_port = matrix_secrets.get("MATRIX_DB_PORT")


def test_prog():
    # Replace these with your actual database credentials

    # conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server},{sql_port};DATABASE={sql_database};UID={sql_username};PWD={sql_password};"
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server},{sql_port};DATABASE={sql_database};UID={sql_username};PWD={sql_password};Connection Timeout=300;"

    conn = pyodbc.connect(conn_string)

    if conn:
        logging.info("Connection test successful.")
        # You can add more code here to interact with the database if needed
        conn.close()
    else:
        logging.error("Connection test failed.")


test_prog()
