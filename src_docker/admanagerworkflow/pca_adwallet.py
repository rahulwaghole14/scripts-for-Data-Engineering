"""
This script processes new line item IDs from Google BigQuery,
fetches associated data from an external API,
and then loads this data back into BigQuery.
It handles the deduplication of IDs and ensures that only new or unprocessed IDs
are fetched and processed.
The script is configured to log its activities to both the console and a file.

Prerequisites:
- Google Cloud BigQuery setup with necessary tables and permissions.
- Necessary environment variables set for API keys and endpoints.
- External functions 'load_config' and 'create_bigquery_client' defined and working.

Usage:
Run the script in an environment where all dependencies
are available and environment variables are set.
"""

import json
import logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import backoff
import requests

from requests.exceptions import Timeout
from google.cloud import bigquery

from common.bigquery.bigquery import create_bigquery_client
from common.aws.aws_secret import get_secret

from .load_config import load_config

# Configure logging to file in the specified directory and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

# Load environment variables
ad_wallet = get_secret("datateam_admanager")
ad_wallet_data = json.loads(ad_wallet)

ad_wallet_key = ad_wallet_data["ad_wallet_api_key"]
ad_wallet_endpoint = ad_wallet_data["ad_wallet_api_endpoint"]


def create_table_if_not_exists(client, table_id, schema):
    """
    Create a BigQuery table if it does not already exist.
    """
    logging.info("Checking if table %s exists", table_id)
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    logging.info("Table %s checked/created", table_id)


def update_current_line_item_ids(bq_client, config, current_table_id):
    """
    Update the current table with new line item IDs from the external API.
    """
    logging.info("Updating current line item IDs from BigQuery")
    # SQL query to select LINE_ITEM_ID from pcagen within the last 90 days
    query = """
    WITH tidy_data AS (
        SELECT
            CASE
                WHEN LINE_ITEM_START_DATE_TIME = '-' THEN NULL
                ELSE SAFE_CAST(LINE_ITEM_START_DATE_TIME AS TIMESTAMP)
            END AS LINE_ITEM_START_DATE_TIME,
            LINE_ITEM_ID
        FROM `{project_id}.{dataset_id}.pca_gen`
    )
    SELECT DISTINCT td.LINE_ITEM_ID
    FROM tidy_data td
    LEFT JOIN `{project_id}.{dataset_id}.Line_Item_ID_Current` tt
    ON td.LINE_ITEM_ID = tt.LINE_ITEM_ID
    WHERE DATE(td.LINE_ITEM_START_DATE_TIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND tt.LINE_ITEM_ID IS NULL
    """.format(
        project_id=config["project_id"], dataset_id=config["dataset_id"]
    )
    try:
        query_job = bq_client.query(query)
        results = query_job.result().to_dataframe()
        if results.empty:
            logging.info("No new line item IDs to update in current table")
            return
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("LINE_ITEM_ID", "INTEGER")]
        )
        bq_client.load_table_from_dataframe(
            results, current_table_id, job_config=job_config
        ).result()
        logging.info("Current line item IDs updated")
    except Exception as err:  # pylint: disable=broad-except
        logging.error("Error updating current line item IDs: %s", err)


def update_used_table(bq_client, line_item_ids, used_table_id):
    """
    Update the used table with processed line item IDs.
    """
    if not line_item_ids:
        return  # If the list is empty, nothing to update

    logging.info("Updating used table with processed line item IDs")

    # Filter out IDs that are already in the used table
    existing_ids_query = f"""
    SELECT LINE_ITEM_ID
    FROM `{used_table_id}`
    WHERE LINE_ITEM_ID IN UNNEST({line_item_ids})
    """
    existing_ids = (
        bq_client.query(existing_ids_query)
        .result()
        .to_dataframe()["LINE_ITEM_ID"]
        .tolist()
    )
    new_ids = [id for id in line_item_ids if id not in existing_ids]

    # Insert new IDs only
    if new_ids:
        data = [{"LINE_ITEM_ID": id} for id in new_ids]
        dataframe = pd.DataFrame(data)
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("LINE_ITEM_ID", "INTEGER")]
        )
        bq_client.load_table_from_dataframe(
            dataframe, used_table_id, job_config=job_config
        ).result()
        logging.info("Used table updated successfully with new IDs")
    else:
        logging.info("No new IDs to update in used table")


@backoff.on_exception(backoff.expo, (ConnectionError, Timeout), max_time=300)
def process_line_item_id(
    line_item_id, proc_ad_wallet_key, proc_ad_wallet_endpoint
):
    """
    Fetch data for a single line item ID from an external API.
    """
    full_url = (
        f"{proc_ad_wallet_endpoint}/screenshots?lineItemId={line_item_id}"
    )
    try:
        response = requests.get(
            full_url, headers={"x-api-key": proc_ad_wallet_key}, timeout=100
        )

        if response.status_code == 200:
            data = json.loads(response.text)
            if isinstance(data, dict):
                data = [data]  # Convert to a list if it's a single dict
            for item in data:
                if "extraAssets" in item and not item["extraAssets"]:
                    item["extraAssets"] = {"thumbnail320": None}
            return data

        if response.status_code == 404:
            logging.info(
                "Line item ID %s: No data found (Status 404)", line_item_id
            )
            return None

        response.raise_for_status()

    except requests.exceptions.HTTPError as e:
        logging.error("HTTP error for line item ID %s: %s", line_item_id, e)
        return None  # or raise an exception if you want to handle this differently
    except (ConnectionError, Timeout) as e:
        logging.error(
            "Network exception for line item ID %s: %s", line_item_id, e
        )
        raise  # Reraise the exception to trigger backoff


def bulk_load_to_bigquery(data, bq_client, table_id, job_config):
    """
    Load data to BigQuery table in bulk.
    """
    if data:
        try:
            job = bq_client.load_table_from_json(
                data, table_id, job_config=job_config
            )
            job.result()
            loaded_ids = [
                item["lineItem"]["id"]
                for item in data
                if "lineItem" in item and "id" in item["lineItem"]
            ]
            logging.info(
                "Loaded data for line item IDs %s to BigQuery table %s",
                loaded_ids,
                table_id,
            )
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Error loading data to BigQuery: %s", e)


def process_new_line_item_ids(
    bq_client,
    current_table_id,
    used_table_id,
    processed_data_table_id,
    batch_size=10,
):
    """
    Process new line item IDs from BigQuery, fetch data from an external API,
    and load the data back to BigQuery.
    """
    logging.info("Fetching new line item IDs for processing")

    # Query to fetch new line item IDs
    new_ids_query = f"""
    SELECT DISTINCT c.LINE_ITEM_ID
    FROM `{current_table_id}` c
    LEFT JOIN `{used_table_id}` u ON c.LINE_ITEM_ID = u.LINE_ITEM_ID
    WHERE u.LINE_ITEM_ID IS NULL
    """
    new_line_item_ids = (
        bq_client.query(new_ids_query)
        .result()
        .to_dataframe()["LINE_ITEM_ID"]
        .tolist()
    )

    # Check if there are no new IDs but still some IDs in current that are not in used
    if not new_line_item_ids:
        check_ids_query = f"""
        SELECT c.LINE_ITEM_ID
        FROM `{current_table_id}` c
        WHERE NOT EXISTS (
            SELECT 1
            FROM `{used_table_id}` u
            WHERE c.LINE_ITEM_ID = u.LINE_ITEM_ID
        )
        """
        unprocessed_ids = (
            bq_client.query(check_ids_query)
            .result()
            .to_dataframe()["LINE_ITEM_ID"]
            .tolist()
        )
        if not unprocessed_ids:
            logging.info("No new line item IDs to process.")
            return

        new_line_item_ids = unprocessed_ids
        logging.info(
            "Processing remaining %s line item IDs not in used table.",
            len(unprocessed_ids),
        )

    total_ids = len(new_line_item_ids)
    logging.info("Total line item IDs fetched: %s", total_ids)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    for i in range(0, total_ids, batch_size):
        batch_ids = new_line_item_ids[i : i + batch_size]
        batch_data = []
        processed_ids = []
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            future_to_id = {
                executor.submit(
                    process_line_item_id, id, ad_wallet_key, ad_wallet_endpoint
                ): id
                for id in batch_ids
            }
            for future in as_completed(future_to_id):
                fut_id = future_to_id[future]
                logging.info("Processing line item ID: %s", fut_id)
                data = future.result()
                if data:
                    batch_data.extend(data)
                    processed_ids.append(fut_id)
                else:
                    logging.info(
                        "No data or error occurred for line item ID: %s",
                        fut_id,
                    )

        # Bulk load to BigQuery
        if batch_data:
            bulk_load_to_bigquery(
                batch_data, bq_client, processed_data_table_id, job_config
            )
            logging.info("Batch data loaded for IDs: %s", processed_ids)
        else:
            logging.info("No data to load for this batch.")

        # Update Line_Item_ID_Used table with processed IDs
        if processed_ids:
            update_used_table(bq_client, processed_ids, used_table_id)
            logging.info("Used table updated with IDs: %s", processed_ids)

        # Log remaining IDs
        remaining_ids = total_ids - (i + batch_size)
        remaining_ids = max(0, remaining_ids)  # Ensure it doesn't go negative
        logging.info("%s line item IDs remaining to process", remaining_ids)


def main():
    """
    Main function to process new line item IDs.
    """

    try:
        start_time = datetime.datetime.now()
        logging.info("Script started")
        config = load_config()
        bq_client = create_bigquery_client(config["google_cred"])
        current_table_id = f"{config['project_id']}.{config['dataset_id']}.Line_Item_ID_Current"
        used_table_id = (
            f"{config['project_id']}.{config['dataset_id']}.Line_Item_ID_Used"
        )
        processed_data_table_id = (
            f"{config['project_id']}.{config['dataset_id']}.adwallet_data"
        )
        create_table_if_not_exists(
            bq_client,
            current_table_id,
            [bigquery.SchemaField("LINE_ITEM_ID", "INTEGER")],
        )
        create_table_if_not_exists(
            bq_client,
            used_table_id,
            [bigquery.SchemaField("LINE_ITEM_ID", "INTEGER")],
        )
        update_current_line_item_ids(bq_client, config, current_table_id)
        process_new_line_item_ids(
            bq_client,
            current_table_id,
            used_table_id,
            processed_data_table_id,
        )
        end_time = datetime.datetime.now()
        logging.info(
            "Script finished. Total runtime: %s", (end_time - start_time)
        )

    except Exception as e:  # pylint: disable=broad-except
        logging.error("Unexpected error: %s", e)


if __name__ == "__main__":
    main()
