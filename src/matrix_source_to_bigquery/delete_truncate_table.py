from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core.retry import Retry
from google.api_core.exceptions import NotFound
from load_config import load_config
from dotenv import load_dotenv
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

def create_bigquery_client():
    """Create a BigQuery client."""
    return bigquery.Client()

def truncate_table(client, dataset_name, table_name):
    """Truncate a BigQuery table by deleting all its rows."""
    query = f"DELETE FROM `{dataset_name}.{table_name}` WHERE TRUE"
    query_job = client.query(query)  # API request
    query_job.result()  # Wait for the job to complete

    logging.info(f"Table {table_name} in dataset {dataset_name} has been truncated.")

def delete_table(client, dataset_name, table_name):
    """Delete a BigQuery table."""
    table_id = f"{dataset_name}.{table_name}"
    try:
        client.delete_table(table_id)  # API request
        logging.info(f"Table {table_name} in dataset {dataset_name} has been deleted.")
    except NotFound:
        logging.info(f"Table {table_name} in dataset {dataset_name} was not found and couldn't be deleted.")


def main():
    # Define the dataset name
    dataset_name = 'cdw_stage_matrix'
    
    # List of tables to truncate
    tables_to_truncate = [
        'tbl_person', 'tbl_ContactNumber', 'tbl_location', 'tbl_country',
        'subscriber', 'subscription', 'subord_cancel', 'tbl_service',
        'tbl_product', 'tbl_period', 'cancellation_reason'
    ]

    # Create a BigQuery client
    client = create_bigquery_client()

    # Truncate each table and its corresponding staging table
    for table_name in tables_to_truncate:
        # Truncate the main table
        #truncate_table(client, dataset_name, table_name)
        
        # Truncate the corresponding staging table
        staging_table_name = f"{table_name}_staging"
        #truncate_table(client, dataset_name, staging_table_name)
        delete_table(client, dataset_name, staging_table_name)

if __name__ == "__main__":
    main()
