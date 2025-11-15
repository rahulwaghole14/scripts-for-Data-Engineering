import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import logging
from load_config import load_config

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Load SQL Server connection parameters from environment variables
sql_server = os.getenv('MATRIX_DB_SERVER')
sql_database = os.getenv('MATRIX_DB_DATABASE')
sql_username = os.getenv('MATRIX_DB_USERNAME')
sql_password = os.getenv('MATRIX_DB_PASSWORD')
sql_port = os.getenv('MATRIX_DB_PORT')

# Load BigQuery configuration
config = load_config()
google_credentials = service_account.Credentials.from_service_account_info(
    config['google_cred'],
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Function to create SQL Server connection
def create_sql_server_connection():
    logging.info("Creating SQL Server connection...")
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server},{sql_port};DATABASE={sql_database};UID={sql_username};PWD={sql_password};"
    return pyodbc.connect(conn_string)

# Function to create BigQuery client
def create_bigquery_client():
    logging.info("Creating BigQuery client...")
    return bigquery.Client(credentials=google_credentials, project=google_credentials.project_id)

# Function to fetch primary key values from SQL Server table
def fetch_sql_server_primary_keys(conn, table_name, primary_key_column):
    logging.info(f"Fetching primary keys from SQL Server table '{table_name}'...")
    cursor = conn.cursor()
    query = f"SELECT {primary_key_column} FROM {table_name}"
    cursor.execute(query)
    return set(row[0] for row in cursor.fetchall())

# Function to fetch primary key values from BigQuery table
def fetch_bigquery_primary_keys(client, dataset_name, table_name, primary_key_column):
    logging.info(f"Fetching primary keys from BigQuery table '{table_name}'...")
    query = f"SELECT {primary_key_column} FROM `{client.project}.{dataset_name}.{table_name}`"
    query_job = client.query(query)
    return set(result[primary_key_column] for result in query_job.result())

# Function to delete rows by primary key in BigQuery table
def delete_rows_bigquery(client, dataset_name, table_name, primary_key_column, primary_keys):
    if not primary_keys:
        logging.info("No primary keys to delete from BigQuery.")
        return

    # Assuming primary_keys are already integers, we directly format them without converting to strings
    keys_str = ', '.join(map(str, primary_keys))  # Directly use integers
    query = f"DELETE FROM `{client.project}.{dataset_name}.{table_name}` WHERE {primary_key_column} IN ({keys_str})"
    
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logging.info(f"Deleted rows with primary keys {keys_str} from BigQuery table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error executing delete query: {e}")

# Main function to execute the process
def main():
    logging.info("Starting row count comparison and cleanup process for batch loading tables...")
    sql_conn = create_sql_server_connection()
    bq_client = create_bigquery_client()

    dataset_name = 'cdw_stage_matrix'
    tables = {
        'complaints': 'comp_id',
        'subscriber': 'subs_pointer',
        'subscription': 'sord_pointer',
        'tbl_person': 'person_pointer',
        'tbl_location': 'loc_pointer',
        'tbl_ContactNumber': 'ContactPointer',
        'subord_cancel': 'ObjectPointer',
    }

    for table_name, primary_key_column in tables.items():
        # Fetch primary keys from both sources
        sql_primary_keys = fetch_sql_server_primary_keys(sql_conn, table_name, primary_key_column)
        bq_primary_keys = fetch_bigquery_primary_keys(bq_client, dataset_name, table_name, primary_key_column)
        
        # Log the count of primary keys from both sources
        logging.info(f"SQL Server '{table_name}' primary key count: {len(sql_primary_keys)}")
        logging.info(f"BigQuery '{table_name}' primary key count: {len(bq_primary_keys)}")

        # Perform row count comparison
        if len(sql_primary_keys) != len(bq_primary_keys):
            logging.info(f"Row count mismatch found in table '{table_name}'. Proceeding to check for missing primary keys...")
            # Identify missing primary keys in SQL Server
            missing_in_sql = bq_primary_keys - sql_primary_keys
            # Log the count of missing primary keys
            logging.info(f"Count of missing primary keys in SQL Server for table '{table_name}': {len(missing_in_sql)}")
            # Delete rows in BigQuery for missing primary keys
            delete_rows_bigquery(bq_client, dataset_name, table_name, primary_key_column, missing_in_sql)
        else:
            logging.info(f"Row counts match for table '{table_name}'. No action required.")

    logging.info("Row count comparison and cleanup process completed.")

if __name__ == "__main__":
    main()

