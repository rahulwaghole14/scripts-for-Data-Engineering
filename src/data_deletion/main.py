"""
data deletor ..^[-_-]^..
data deletion script will delete data for email or userid matches,
marketing_ids in sqlserver and bigquery cdw
please add new tables if you want to delete from them and list
their matching fields in dictionaries below
"""

import json
import logging
import os
import base64

from dotenv import load_dotenv
import pandas as pd
from create_uuid import generate_uuid
from google.oauth2.service_account import Credentials
from compare_delete import (
    data_deletion_ids_to_bigquery,
    compare_and_delete_bigquery,
    delete_from_vault_bigquery,
    drop_table_bigquery,
)
from configuration import (
    source_tables_with_keys_google,
    bigquery_vault,
)

# load environment variables from .env file
load_dotenv()

# Parameters
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
sql_driver = os.getenv("DB_DRIVER")
uuid_namespace = os.getenv("UUID_NAMESPACE")
encoded_cred = os.getenv("GOOGLE_CLOUD_CRED_BASE64")

# print(google_cred)
# google_cred = Credentials.from_service_account_info(google_cred)
FILE = "src/data_deletion/to_delete.csv"

decoded_cred = base64.b64decode(encoded_cred).decode("utf-8")
google_cred = json.loads(decoded_cred)


google_cred = Credentials.from_service_account_info(
    google_cred,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    dataframe = pd.read_csv(FILE)
    logging.info("[Prepare Data Deletion Table]: dataframe created from csv")
    dataframe["user__id"] = dataframe["user__id"].astype("Int64", errors="raise")
    dataframe = dataframe[dataframe["email"].str.contains("@", na=False)]
    logging.info("[Prepare Data Deletion Table]: removing invalid email done")
    dataframe["email"] = dataframe["email"].str.lower().str.strip()
    # create marketing ids
    dataframe["marketing_id_email"] = dataframe["email"].apply(
        lambda x: generate_uuid(x, uuid_namespace)
    )
    # Apply generate_uuid to 'user__id' only if it's not NaN
    dataframe["marketing_id"] = dataframe.apply(
        lambda row: generate_uuid(row["user__id"], uuid_namespace)
        if pd.notnull(row["user__id"])
        else None,
        axis=1,
    )
except ValueError as e:
    logging.info("[Prepare Data Deletion Table]: ValueError Error: %s", e)
except Exception as error:
    logging.info("[Prepare Data Deletion Table]: %s", error)

# Set BigQuery dataset and table ids
DATASET_ID_BIGQUERY = "cdw_stage"
TABLE_ID_BIGQUERY = "data_deletion"
temp_table_id_bigquery = f"{DATASET_ID_BIGQUERY}.{TABLE_ID_BIGQUERY}"

# Set SQLServer dataset and table ids
TEMP_TABLE_ID_SQLSERVER = "stage.temp.data_deletion"

# Create the connection string and engine for sql server
conn = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    f"?driver={sql_driver}&TrustServerCertificate=yes"
)

# load data deletion ids to temp tables
# data_deletion_ids_to_sqlserver(dataframe, conn)
data_deletion_ids_to_bigquery(dataframe, temp_table_id_bigquery, google_cred)

# Delete from Vaults
delete_from_vault_bigquery(bigquery_vault, google_cred)
# Delete from Stage
compare_and_delete_bigquery(
    temp_table_id_bigquery, source_tables_with_keys_google, google_cred
)
# vault
# delete_from_sql_vault(sqlserver_vault, conn)
# stage
# compare_and_delete_sqlserver(TEMP_TABLE_ID_SQLSERVER, source_tables_with_keys_sqlserver, conn)

# drop temp tables
drop_table_bigquery(google_cred, DATASET_ID_BIGQUERY, TABLE_ID_BIGQUERY)
# drop_table_sqlserver(conn)
