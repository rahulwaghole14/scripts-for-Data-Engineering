import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from load_config import load_config  
from dotenv import load_dotenv
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

def create_sql_server_connection():
    server = os.getenv('MATRIX_DB_SERVER')
    database = os.getenv('MATRIX_DB_DATABASE')
    username = os.getenv('MATRIX_DB_USERNAME')
    password = os.getenv('MATRIX_DB_PASSWORD')
    portnumber = os.getenv('MATRIX_DB_PORT')
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{portnumber};DATABASE={database};UID={username};PWD={password};"
    return pyodbc.connect(conn_string)

def create_bigquery_client():
    config = load_config()  
    credentials = service_account.Credentials.from_service_account_info(
        config['google_cred'],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

tables_to_load = [
    'complaints', 'subord_cancel', 'subscriber', 'subscription', 'tbl_person', 'tbl_location', 'tbl_ContactNumber',
    'tbl_country', 'tbl_service', 'tbl_product', 'tbl_period', 'cancellation_reason'
]

# Use the created SQL Server connection
sql_server_conn = create_sql_server_connection()

# Create BigQuery client
bigquery_client = create_bigquery_client()
bigquery_project_id = bigquery_client.project  

def get_sql_server_count(table_name):
    with sql_server_conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]

def get_bigquery_count(table_name, dataset_name):
    query = f"SELECT COUNT(*) AS count FROM `{bigquery_project_id}.{dataset_name}.{table_name}`"
    query_job = bigquery_client.query(query)
    results = query_job.result()
    for row in results:
        return row['count']


your_dataset_name = 'cdw_stage_matrix' 

# Compare counts
for table_name in tables_to_load:
    sql_server_count = get_sql_server_count(table_name)
    bigquery_count = get_bigquery_count(table_name, your_dataset_name)
    logging.info(f"Table '{table_name}': SQL Server count = {sql_server_count}, BigQuery count = {bigquery_count}")
    if sql_server_count != bigquery_count:
        logging.warning(f"Warning: Row count mismatch for table '{table_name}'!")
