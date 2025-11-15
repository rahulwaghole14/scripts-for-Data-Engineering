import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
from load_config import load_config
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Set up SQL Server connection parameters
sql_server = os.getenv('MATRIX_DB_SERVER')
sql_database = os.getenv('MATRIX_DB_DATABASE')
sql_username = os.getenv('MATRIX_DB_USERNAME')
sql_password = os.getenv('MATRIX_DB_PASSWORD')
sql_port = os.getenv('MATRIX_DB_PORT')

# BigQuery configuration
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

# Function to compare primary keys between SQL Server and BigQuery tables
def compare_primary_keys(sql_conn, bq_client, dataset_name, sql_table_name, bq_table_name, primary_key_column):
    logging.info(f"Comparing primary keys between SQL Server and BigQuery for table '{sql_table_name}'...")
    sql_primary_keys = fetch_sql_server_primary_keys(sql_conn, sql_table_name, primary_key_column)
    bq_primary_keys = fetch_bigquery_primary_keys(bq_client, dataset_name, bq_table_name, primary_key_column)

    # Find differences
    missing_in_sql = bq_primary_keys - sql_primary_keys
    missing_in_bq = sql_primary_keys - bq_primary_keys

    return missing_in_sql, missing_in_bq

# Main function to execute the comparison for all tables
def main():
    logging.info("Starting primary key comparison process...")
    sql_conn = create_sql_server_connection()
    bq_client = create_bigquery_client()
    
    dataset_name = 'cdw_stage_matrix'
    hash_key_tables = [
        'complaints', 'subscriber', 'subscription', 'tbl_person', 
        'tbl_ContactNumber', 'subord_cancel', 'tbl_location'
    ]
    primary_key_columns = {
        'complaints': 'comp_id',
        'subscriber': 'subs_pointer',
        'subscription': 'sord_pointer',
        'tbl_person': 'person_pointer',
        'tbl_location': 'loc_pointer',  
        'tbl_ContactNumber': 'ContactPointer', 
        'subord_cancel': 'ObjectPointer', 
    }

    for table in hash_key_tables:
        primary_key_column = primary_key_columns[table]
        missing_in_sql, missing_in_bq = compare_primary_keys(sql_conn, bq_client, dataset_name, table, table, primary_key_column)
        
        logging.info(f"For table '{table}':")
        logging.info(f"  Primary keys missing in SQL Server (present in BigQuery): {missing_in_sql}")
        logging.info(f"  Primary keys missing in BigQuery (present in SQL Server): {missing_in_bq}\n")

    logging.info("Primary key comparison process completed.")

if __name__ == "__main__":
    main()
