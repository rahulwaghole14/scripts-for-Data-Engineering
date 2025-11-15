import requests
import json
import pandas as pd
import hashlib
import time
import logging
import warnings
import datetime
import os
import sys
from tqdm import tqdm
from contextlib import contextmanager
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

# Configuration
base_url = 'https://dashboard.presspatron.com'
headers = {'Authorization': 'Token 20039f24-0dd8-4df8-8b02-92b1ef601b71'}
warnings.filterwarnings("ignore")

# Setup logging
log_path = os.path.join(os.path.expanduser("~"), "presspatron_to_bigquery/logs")
os.makedirs(log_path, exist_ok=True)
log_file = os.path.join(log_path, f"PressPatron_{datetime.date.today()}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ])

# Function to fetch a single page with retries and rate limiting
def fetch_page(url, page_number, retries=5, delay=2):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                logging.warning(f"Rate limit exceeded on page {page_number}, retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                continue
            response.raise_for_status()
            response_dict = response.json()
            temp = pd.DataFrame(response_dict['items'])
            logging.info(f"Downloaded page {page_number}")
            return temp
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get page {page_number}: {e}")
            time.sleep(delay)
            delay *= 2
    return pd.DataFrame()

# Function to download data in batches
@contextmanager
def tqdm_job(*args, **kwargs):
    pbar = tqdm(*args, **kwargs)
    yield pbar
    pbar.close()

def download_dataframe_with_pagelimit(resource, start_page, batch_size):
    page_number = start_page
    while True:
        batch_df = pd.DataFrame()
        with tqdm_job(range(batch_size), desc=f"Downloading {resource} pages {page_number} to {page_number + batch_size - 1}") as pbar:
            for _ in pbar:
                url = f"{base_url}/api/v1/{resource}?page={page_number}"
                temp_df = fetch_page(url, page_number)
                if temp_df.empty:
                    yield batch_df
                    return
                batch_df = pd.concat([batch_df, temp_df], ignore_index=True)
                page_number += 1
        yield batch_df

def ensure_dataset_in_australia(client, dataset_id):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        if dataset.location != "australia-southeast1":
            raise Exception("Dataset found but in the wrong location.")
        logging.info(f"Dataset '{dataset_id}' exists in Australia.")
    except NotFound:
        logging.info(f"Dataset '{dataset_id}' not found. Creating in Australia.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "australia-southeast1"
        client.create_dataset(dataset, exists_ok=True)
    except Exception as e:
        logging.error(f"Error with dataset: {e}")

def create_table_if_not_exists(client, table_id, bq_schema, cluster_fields=[]):
    try:
        client.get_table(table_id)
        logging.info(f"Table '{table_id}' already exists.")
    except NotFound:
        logging.info(f"Creating table '{table_id}' with clustering on {cluster_fields}.")
        table = bigquery.Table(table_id, schema=bq_schema)
        if cluster_fields:
            table.clustering_fields = cluster_fields
        client.create_table(table)

def create_or_update_table_with_data(df, client, table_id, bq_schema):
    if not df.empty:
        try:
            # Generate hash key for each row based on all columns
            df['hash_key'] = df.apply(lambda row: hashlib.md5(pd.util.hash_pandas_object(row, index=False).values).hexdigest(), axis=1)

            # Sort the dataframe by createdAt and updatedAt
            df = df.sort_values(by=['createdAt', 'updatedAt'])

            # Load data into a temporary table
            temp_table_id = table_id + "_temp"
            create_table_if_not_exists(client, temp_table_id, bq_schema)

            logging.info(f"Loading DataFrame with {df.shape[0]} rows to {temp_table_id}")
            job_config = bigquery.LoadJobConfig(
                schema=bq_schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate data in the temp table
            )
            job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            # Merge temp table into the main table
            merge_query = f"""
            MERGE `{table_id}` T
            USING `{temp_table_id}` S
            ON T.hash_key = S.hash_key
            WHEN MATCHED THEN
              UPDATE SET
                {', '.join([f'T.{col.name} = S.{col.name}' for col in bq_schema])}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join([col.name for col in bq_schema])})
              VALUES ({', '.join([f'S.{col.name}' for col in bq_schema])})
            """
            query_job = client.query(merge_query)
            query_job.result()
            logging.info(f"Merged data from {temp_table_id} to {table_id}")

            # Delete the temp table
            delete_table(client, temp_table_id)

        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
    else:
        logging.info("No dataframe to load")

def delete_table(client, table_id):
    try:
        client.delete_table(table_id)
        logging.info(f"Deleted table '{table_id}'")
    except NotFound:
        logging.info(f"Table '{table_id}' not found. Nothing to delete.")

def load_new_data(file, project_id, dataset_id, client, base_url, headers, bq_schema, page_limit=100):
    latest_timestamp_query = f"""
    SELECT MAX(updatedAt) as latest_timestamp
    FROM `{project_id}.{dataset_id}.{file}_refresh`
    """
    query_job = client.query(latest_timestamp_query)
    results = query_job.result()
    latest_timestamp = None
    for row in results:
        latest_timestamp = row.latest_timestamp

    if latest_timestamp:
        latest_timestamp = pd.to_datetime(latest_timestamp)
        new_data_df = pd.DataFrame()
        page_number = 1
        flag = True

        # Fetch new data pages
        while flag and page_number <= page_limit:
            try:
                url = f"{base_url}/api/v1/{file}?page={page_number}&updatedAt__gt={latest_timestamp.isoformat()}"
                res = requests.get(url, headers=headers)
                res.raise_for_status()
                response_dict = res.json()
                temp = pd.DataFrame(response_dict['items'])
                if temp.empty:
                    flag = False
                    break
                new_data_df = pd.concat([new_data_df, temp], ignore_index=True)
                page_number += 1
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch new data for '{file}': {e}")
                flag = False

        if not new_data_df.empty:
            # Filter data by updatedAt > latest_timestamp
            new_data_df['updatedAt'] = pd.to_datetime(new_data_df['updatedAt'])
            new_data_df = new_data_df[new_data_df['updatedAt'] > latest_timestamp]

            # Process and load data to staging table
            all_columns = list(new_data_df)
            new_data_df[all_columns] = new_data_df[all_columns].fillna("").astype(str)
            new_data_df['load_dts'] = str(pd.Timestamp.now())
            
            # Generate hash key for each row based on all columns
            new_data_df['hash_key'] = new_data_df.apply(lambda row: hashlib.md5(pd.util.hash_pandas_object(row, index=False).values).hexdigest(), axis=1)
            
            # Create staging table if it doesn't exist
            staging_table_id = f"{project_id}.{dataset_id}.{file}_refresh_staging"
            create_table_if_not_exists(client, staging_table_id, bq_schema[file])
            
            # Load data into staging table
            job_config = bigquery.LoadJobConfig(
                schema=bq_schema[file],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate data in the staging table
            )
            job = client.load_table_from_dataframe(new_data_df, staging_table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            # Merge staging table into target table
            backfill_table_id = f"{project_id}.{dataset_id}.{file}_refresh"
            merge_query = f"""
            MERGE `{backfill_table_id}` T
            USING `{staging_table_id}` S
            ON T.hash_key = S.hash_key
            WHEN MATCHED AND T.updatedAt != S.updatedAt THEN
              UPDATE SET
                {', '.join([f'T.{col.name} = S.{col.name}' for col in bq_schema[file] if col.name != 'hash_key'])}
            WHEN NOT MATCHED THEN
              INSERT ({', '.join([col.name for col in bq_schema[file]])})
              VALUES ({', '.join([f'S.{col.name}' for col in bq_schema[file]])})
            """
            query_job = client.query(merge_query)
            query_job.result()
            logging.info(f"Merged data from {staging_table_id} to {backfill_table_id}")

            # Log the number of rows merged
            rows_merged_query = f"SELECT COUNT(*) AS merged_rows FROM `{backfill_table_id}`"
            merged_rows_job = client.query(rows_merged_query)
            merged_rows_result = merged_rows_job.result()
            for row in merged_rows_result:
                logging.info(f"Total rows in {backfill_table_id}: {row.merged_rows}")

            # Delete the staging table
            delete_table(client, staging_table_id)



def backfill_data(files, project_id, dataset_id, client, base_url, headers, bq_schema):
    batch_size = 200  # Set batch size to 200

    for file in files:
        backfill_table_id = project_id + '.' + dataset_id + '.' + file + '_refresh'
        staging_table_id = f"{backfill_table_id}_staging"

        # Create target table if it doesn't exist
        create_table_if_not_exists(client, backfill_table_id, bq_schema[file], cluster_fields=['createdAt', 'updatedAt', 'hash_key'])

        # Create staging table if it doesn't exist
        create_table_if_not_exists(client, staging_table_id, bq_schema[file])

        # Download and load data into the staging table
        for df_new in download_dataframe_with_pagelimit(file, 1, batch_size):
            all_columns = list(df_new)
            df_new[all_columns] = df_new[all_columns].fillna("").astype(str)
            df_new['load_dts'] = str(pd.Timestamp.now())
            create_or_update_table_with_data(df_new, client, staging_table_id, bq_schema[file])

        # Merge staging table into target table
        merge_query = f"""
        MERGE `{backfill_table_id}` T
        USING `{staging_table_id}` S
        ON T.hash_key = S.hash_key
        WHEN NOT MATCHED THEN
          INSERT ROW
        """
        query_job = client.query(merge_query)
        query_job.result()
        logging.info(f"Merged data from {staging_table_id} to {backfill_table_id}")

        # Log the number of rows merged
        rows_merged_query = f"SELECT COUNT(*) AS merged_rows FROM `{backfill_table_id}`"
        merged_rows_job = client.query(rows_merged_query)
        merged_rows_result = merged_rows_job.result()
        for row in merged_rows_result:
            logging.info(f"Total rows in {backfill_table_id}: {row.merged_rows}")

        # Delete the staging table
        delete_table(client, staging_table_id)

        # Log completion of data loading for this table
        logging.info(f"Completed data loading for '{file}'.")

if __name__ == "__main__":
    # Parameters for BigQuery
    path = os.getcwd()
    project_id = "hexa-data-report-etl-prod"
    dataset_id = 'press_patron_api_data'
    key_path = os.path.join(path, "secrets/hexa-data-report-etl-prod-5b93b81b644e.json")
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    ensure_dataset_in_australia(client, dataset_id)
    
    # BigQuery schemas
    schema_users = [
        bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("newsletterSubscribed", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("anonymous", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("firstName", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("lastName", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("address", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("hash_key", bigquery.enums.SqlTypeNames.STRING)
    ]
    schema_subscriptions = [
        bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("subscriptionId", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cancellationAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("grossAmount", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("frequency", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("subscriptionStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("rewardSelected", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("metadata", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("urlSource", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("hash_key", bigquery.enums.SqlTypeNames.STRING)
    ]
    schema_transactions = [
        bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("transactionId", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("grossAmount", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("cardIssueCountry", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING), 
        bigquery.SchemaField("subscriptionId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("frequency", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("processorCreditCardFee", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("netAmount", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("totalFees", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("processorBankTransferFee", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("pressPatronCommission", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("rewardSelected", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("metadata", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("urlSource", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("paymentStatus", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("pressPatronCommissionSalesTax", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("hash_key", bigquery.enums.SqlTypeNames.STRING)
    ]

    # List of resources
    files = [
             # "subscriptions"
            #  , "transactions", 
             "users"
             ]

    # Schema for each resource
    bq_schema = {
        "subscriptions": schema_subscriptions, 
        "users": schema_users, 
        "transactions": schema_transactions
    }

    if len(sys.argv) != 2 or sys.argv[1] not in ["backfill", "update"]:
        print("Usage: script.py [backfill|update]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "backfill":
        backfill_data(files, project_id, dataset_id, client, base_url, headers, bq_schema)
    elif mode == "update":
        for file in files:
            load_new_data(file, project_id, dataset_id, client, base_url, headers, bq_schema, page_limit=100)

    # Ensure the script exits properly
    sys.exit(0)
