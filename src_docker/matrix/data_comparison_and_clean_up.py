import os
import json
import sys
import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.aws.aws_secret import get_secret
from common.logging.logger import logger, log_start, log_end

# Initialize the custom logger
log_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "log")
logger = logger("row_count_comparison", log_dir)

# Load the Google credentials from AWS Secret Manager
google_creds_json = get_secret("datateam_google_prod")
google_creds = json.loads(google_creds_json)

def load_config():
    config = {
        "google_cred": google_creds,
        "project_id": google_creds["project_id"],
    }
    return config

# Load SQL Server connection parameters from environment variables
secret_value = get_secret("datateam_CDW_and_Matrix_Database")
matrix_secrets = json.loads(secret_value)
sql_server = matrix_secrets.get("MATRIX_DB_SERVER_IP_NEW")
sql_database = matrix_secrets.get("MATRIX_DB_DATABASE")
sql_username = matrix_secrets.get("MATRIX_DB_USERNAME")
sql_password = matrix_secrets.get("MATRIX_DB_PASSWORD")
sql_port = matrix_secrets.get("MATRIX_DB_PORT")

# Load BigQuery configuration
config = load_config()
google_credentials = service_account.Credentials.from_service_account_info(
    config["google_cred"],
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Function to create SQL Server connection
def create_sql_server_connection():
    logger.info("Creating SQL Server connection...")
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server},{sql_port};DATABASE={sql_database};UID={sql_username};PWD={sql_password};"
    return pyodbc.connect(conn_string)

# Function to create BigQuery client
def create_bigquery_client():
    logger.info("Creating BigQuery client...")
    return bigquery.Client(
        credentials=google_credentials, project=google_credentials.project_id
    )

# Function to fetch primary key values from SQL Server table
def fetch_sql_server_primary_keys(conn, table_name, primary_key_column):
    logger.info(f"Fetching primary keys from SQL Server table '{table_name}'...")
    cursor = conn.cursor()
    query = f"SELECT {primary_key_column} FROM {table_name}"
    cursor.execute(query)
    return set(row[0] for row in cursor.fetchall())

# Function to fetch primary key values from BigQuery table
def fetch_bigquery_primary_keys(client, dataset_name, table_name, primary_key_column):
    logger.info(f"Fetching primary keys from BigQuery table '{table_name}'...")
    query = f"SELECT {primary_key_column} FROM `{client.project}.{dataset_name}.{table_name}`"
    query_job = client.query(query)
    return set(result[primary_key_column] for result in query_job.result())

# Function to delete rows by primary key in BigQuery table
def delete_rows_bigquery(client, dataset_name, table_name, primary_key_column, primary_keys):
    if not primary_keys:
        logger.info("No primary keys to delete from BigQuery.")
        return

    keys_str = ", ".join(map(str, primary_keys))
    query = f"DELETE FROM `{client.project}.{dataset_name}.{table_name}` WHERE {primary_key_column} IN ({keys_str})"

    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logger.info(f"Deleted rows with primary keys {keys_str} from BigQuery table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error executing delete query: {e}")

# Main function to execute the process
def main():
    
    logger.info("Starting row count comparison and cleanup process for batch loading tables...")
    
    try:
        sql_conn = create_sql_server_connection()
        bq_client = create_bigquery_client()

        dataset_name = "cdw_stage_matrix"
        tables = {
            "complaints": "comp_id",
            "subscriber": "subs_pointer",
            "subscription": "sord_pointer",
            "tbl_person": "person_pointer",
            "tbl_location": "loc_pointer",
            "tbl_ContactNumber": "ContactPointer",
            "subord_cancel": "ObjectPointer",
        }

        for table_name, primary_key_column in tables.items():
            sql_primary_keys = fetch_sql_server_primary_keys(sql_conn, table_name, primary_key_column)
            bq_primary_keys = fetch_bigquery_primary_keys(bq_client, dataset_name, table_name, primary_key_column)

            logger.info(f"SQL Server '{table_name}' primary key count: {len(sql_primary_keys)}")
            logger.info(f"BigQuery '{table_name}' primary key count: {len(bq_primary_keys)}")

            if len(sql_primary_keys) != len(bq_primary_keys):
                logger.info(f"Row count mismatch found in table '{table_name}'. Proceeding to check for missing primary keys...")
                missing_in_sql = bq_primary_keys - sql_primary_keys
                logger.info(f"Count of missing primary keys in SQL Server for table '{table_name}': {len(missing_in_sql)}")
                delete_rows_bigquery(bq_client, dataset_name, table_name, primary_key_column, missing_in_sql)
            else:
                logger.info(f"Row counts match for table '{table_name}'. No action required.")

        logger.info("Row count comparison and cleanup process completed.")
    except Exception as e:
        logger.error(f"An error occurred during script execution: {str(e)}")
        sys.exit(1)
    finally:
        if 'sql_conn' in locals():
            sql_conn.close()
        log_end(logger, "Matrix Data Process")

if __name__ == "__main__":
    main()