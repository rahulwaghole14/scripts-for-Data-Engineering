# # import pyodbc
# # from google.cloud import bigquery
# # from google.oauth2 import service_account
# # import logging
# # from datetime import datetime
# # from decimal import Decimal
# # from google.api_core.retry import Retry
# # from load_config import load_config
# # from dotenv import load_dotenv
# # import os

# # # Initialize logging
# # logging.basicConfig(level=logging.INFO)

# # # Load environment variables
# # load_dotenv()

# # # Set up basic logging
# # logging.basicConfig(level=logging.INFO)

# # # Missing comp_id values identified from the comparison
# # missing_comp_ids = [
# #     8351106, 9287970, 8345604, 10989253, 9288422, 8350439, 9286920, 
# #     8345705, 15438850, 15438853, 9288782, 15438927, 9288368, 10989360, 
# #     10988535, 8348539, 15439229, 9288863
# # ]

# # def create_sql_server_connection():
# #     server = os.getenv('MATRIX_DB_SERVER')
# #     database = os.getenv('MATRIX_DB_DATABASE')
# #     username = os.getenv('MATRIX_DB_USERNAME')
# #     password = os.getenv('MATRIX_DB_PASSWORD')
# #     portnumber = os.getenv('MATRIX_DB_PORT')
# #     conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{portnumber};DATABASE={database};UID={username};PWD={password};"
# #     return pyodbc.connect(conn_string)

# # def create_bigquery_client():
# #     config = load_config()
# #     credentials = service_account.Credentials.from_service_account_info(
# #         config['google_cred'],
# #         scopes=["https://www.googleapis.com/auth/cloud-platform"],
# #     )
# #     return bigquery.Client(credentials=credentials, project=credentials.project_id)

# # def convert_to_serializable(value):
# #     """Convert non-serializable types (datetime, Decimal) to a string format."""
# #     if isinstance(value, datetime):
# #         return value.isoformat()
# #     elif isinstance(value, Decimal):
# #         return str(value)
# #     return value

# # def prepare_data_for_bigquery(rows, cursor_description):
# #     """Prepares data fetched from SQL Server for BigQuery insertion."""
# #     data_for_bigquery = []
# #     for row in rows:
# #         processed_row = {desc[0]: convert_to_serializable(row[i]) for i, desc in enumerate(cursor_description)}
# #         data_for_bigquery.append(processed_row)
# #     return data_for_bigquery

# # def create_or_update_bigquery_table(client, dataset_name, table_name, data_for_bigquery):
# #     """Uploads data to BigQuery, handling retries for transient errors."""
# #     table_id = f"{client.project}.{dataset_name}.{table_name}"
# #     retry = Retry(deadline=30)
# #     errors = client.insert_rows_json(table_id, data_for_bigquery, retry=retry)
# #     if errors:
# #         logging.error(f"Encountered errors while inserting rows into {table_name}: {errors}")
# #     else:
# #         logging.info(f"Successfully loaded {len(data_for_bigquery)} rows into {table_name}")

# # def refresh_missing_comp_ids(sql_conn, bq_client, dataset_name, table_name, missing_ids):
# #     """Fetches and uploads rows for missing comp_ids from SQL Server to BigQuery."""
# #     cursor = sql_conn.cursor()
# #     placeholders = ','.join(['?'] * len(missing_ids))
# #     query = f"SELECT * FROM {table_name} WHERE comp_id IN ({placeholders})"
# #     cursor.execute(query, missing_ids)
# #     rows = cursor.fetchall()

# #     if rows:
# #         logging.info(f"Fetched {len(rows)} rows for manual refresh in {table_name}.")
# #         data_for_bigquery = prepare_data_for_bigquery(rows, cursor.description)
# #         create_or_update_bigquery_table(bq_client, dataset_name, table_name, data_for_bigquery)
# #         logging.info(f"Completed manual refresh for missing comp_ids in {table_name}.")
# #     else:
# #         logging.info("No rows fetched for manual refresh.")

# # def main():
# #     sql_conn = create_sql_server_connection()
# #     bq_client = create_bigquery_client()

# #     # Specify your BigQuery dataset name
# #     dataset_name = 'cdw_stage_matrix'
# #     table_name = 'complaints'

# #     refresh_missing_comp_ids(sql_conn, bq_client, dataset_name, table_name, missing_comp_ids)

# # if __name__ == "__main__":
# #     main()


# # ###### test for latest value #####


# # import pyodbc
# # from google.cloud import bigquery
# # from google.oauth2 import service_account
# # import logging
# # from load_config import load_config
# # from dotenv import load_dotenv

# # load_dotenv()

# # def create_bigquery_client():
# #     config = load_config()
# #     google_cred = config['google_cred']
# #     credentials = service_account.Credentials.from_service_account_info(
# #         google_cred,
# #         scopes=["https://www.googleapis.com/auth/cloud-platform"]
# #     )
# #     client = bigquery.Client(credentials=credentials, project=credentials.project_id)
# #     return client

# # def get_latest_value(client, dataset_name, table_name, column_name, is_timestamp=True):
# #     if is_timestamp:
# #         query = f"SELECT MAX(CAST({column_name} AS STRING)) AS latest_value FROM `{client.project}.{dataset_name}.{table_name}`"
# #     else:
# #         query = f"SELECT MAX({column_name}) AS latest_value FROM `{client.project}.{dataset_name}.{table_name}`"
# #     query_job = client.query(query)
# #     results = query_job.result()
# #     for row in results:
# #         return row.latest_value

# # def main():
# #     # Create BigQuery client
# #     client = create_bigquery_client()

# #     # Define the dataset and table details
# #     dataset_name = 'cdw_stage_matrix'
# #     table_name = 'subord_cancel'
# #     column_name = 'ObjectPointer'
# #     is_timestamp = True  # Set this to False because ObjectPointer is not a timestamp

# #     # Get the latest ObjectPointer value
# #     latest_value = get_latest_value(client, dataset_name, table_name, column_name, is_timestamp)
# #     print(f"The latest ObjectPointer value for {table_name} is: {latest_value}")

# # if __name__ == "__main__":
# #     main()


# #### test for pull out the schema ####
# import pyodbc
# from google.cloud import bigquery
# from google.oauth2 import service_account
# from datetime import datetime
# from decimal import Decimal
# from google.api_core.retry import Retry
# from load_config import load_config
# from dotenv import load_dotenv
# import os

# load_dotenv() 

# def create_bigquery_client():
#     config = load_config()
#     credentials = service_account.Credentials.from_service_account_info(
#         config['google_cred'],
#         scopes=["https://www.googleapis.com/auth/cloud-platform"],
#     )
#     return bigquery.Client(credentials=credentials, project=credentials.project_id)

# # Function to fetch the schema of a BigQuery table
# def get_table_schema(client, dataset_name, table_name):
#     """
#     Fetch the schema of a BigQuery table.
    
#     Parameters:
#     - client: BigQuery client object
#     - dataset_name: Name of the dataset
#     - table_name: Name of the table
    
#     Returns:
#     - A list of bigquery.SchemaField objects representing the table schema
#     """
#     table_id = f"{client.project}.{dataset_name}.{table_name}"
#     table = client.get_table(table_id)  # API request
#     return table.schema

# def print_table_schemas(client, dataset_name, tables):
#     """
#     Print the schema for a list of tables.
    
#     Parameters:
#     - client: BigQuery client object
#     - dataset_name: Name of the dataset
#     - tables: A list of table names
#     """
#     for table_name in tables:
#         print(f"Schema for {table_name}:")
#         schema = get_table_schema(client, dataset_name, table_name)
#         for field in schema:
#             print(f"  Field name: {field.name}, Field type: {field.field_type}, Mode: {field.mode}")
#         print("\n")  # Add a newline for better readability between tables

# # Initialize the BigQuery client
# client = create_bigquery_client()

# # Your dataset name
# dataset_name = 'cdw_stage_matrix'

# # List of tables for full refresh and hash key-based updates
# full_refresh_tables = ['tbl_country', 'tbl_service', 'tbl_product', 'tbl_period', 'cancellation_reason']
# hash_key_tables = ['complaints', 'subscriber', 'subscription', 'tbl_person', 'tbl_ContactNumber', 'subord_cancel']

# # Combine all tables into a single list for simplicity
# all_tables = full_refresh_tables + hash_key_tables

# # Print the schema for all tables
# print_table_schemas(client, dataset_name, all_tables)
