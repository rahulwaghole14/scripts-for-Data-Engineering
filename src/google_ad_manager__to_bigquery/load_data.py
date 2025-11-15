"""
This script is designed to automate the process of fetching data from Google Ad Manager,
processing it, and loading it into Google BigQuery. It handles tasks like checking if data
is already loaded for specific dates, deleting data for given date ranges, and loading new data.
The script uses the googleads and google-cloud-bigquery libraries for API interactions.

Prerequisites:
- Google Cloud BigQuery and Google Ad Manager setup with necessary permissions.
- Relevant Python libraries installed: googleads, google-cloud-bigquery, pandas.
- Configuration files for Google Ad Manager and environment variables set up.

Usage:
- The script can be run directly or imported as a module in other scripts.
- It is designed to work with a specific configuration and requires a setup
for Google Ad Manager and BigQuery.
"""

import time
import logging
import os
from datetime import datetime, timedelta
from googleads import ad_manager
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest

from google_ad_manager__to_bigquery.bin.run_saved_report import (
    run_report,
    run_report_validated,
)

from google_ad_manager__to_bigquery.load_config import load_config
from a_common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    load_data_to_bigquery_append,
    create_bigquery_client,
    create_table_if_not_exists as ctie_bq,
)


# Configure logging to file and console
def setup_logger():
    """
    Set up logging configuration
    """
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(
                os.path.join(log_directory, "data_load_log.log")
            ),
            logging.StreamHandler(),
        ],
    )
    logger_obj = logging.getLogger()
    return logger_obj


logger = setup_logger()

# Load configuration
config = load_config()
# Initialize the BigQuery client
bq_client = create_bigquery_client(config["google_cred"])


# Initialize the Google Ad Manager client
gam_client = ad_manager.AdManagerClient.LoadFromStorage()
report_service = gam_client.GetService(
    "ReportService", version=config["api_version"]
)


def create_bigquery_dataset_if_not_exists(
    client, project_id, dataset_id, location="US"
):
    """
    Create a BigQuery dataset if it doesn't already exist
    """
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = (
        "australia-southeast1"  # Set the location for the dataset as a string
    )

    try:
        client.get_dataset(dataset)  # Make an API request.
        logging.info("Dataset %s already exists.", dataset_id)
    except NotFound:
        logging.info("Dataset %s not found. Creating new dataset.", dataset_id)
        dataset = client.create_dataset(dataset)  # Make an API request.
        logging.info(
            "Create dataset %s in project %s with location %s",
            dataset.project,
            dataset.dataset_id,
            location,
        )


def create_bigquery_table_if_not_exists(client, full_table_id, dataframe):
    """
    Create a BigQuery table if it doesn't already exist
    """
    try:
        # Attempt to get the table using the full table ID
        client.get_table(full_table_id)
        logging.info("Table %s already exists.", full_table_id)
    except NotFound:
        logging.info(
            "Table %s not found. Creating new table with schema derived from DataFrame.",
            full_table_id,
        )
        # Derive the schema from the dataframe
        schema = [
            bigquery.SchemaField(
                name,
                "STRING"
                if dtype.name == "object"
                else "INTEGER"
                if "int" in dtype.name
                else "FLOAT"
                if "float" in dtype.name
                else "BOOLEAN"
                if dtype.name == "bool"
                else "TIMESTAMP"
                if dtype.name == "datetime64[ns]"
                else "BYTES"
                if dtype.name == "bytes"
                else "STRING",
            )
            for name, dtype in zip(dataframe.columns, dataframe.dtypes)
        ]
        # Create a new table object with the full table ID and the derived schema
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)
        logging.info("Created table %s", full_table_id)


def table_exists(client, table_id):
    """
    Check if a BigQuery table exists
    """
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def data_already_loaded(client, table_id, check_date):
    """
    Check if data for a specific date is already loaded in a BigQuery table
    """
    if not table_exists(client, table_id):
        logging.info(
            "Table %s does not exist. Data for %s considered not loaded.",
            table_id,
            check_date.strftime("%Y-%m-%d"),
        )
        return False

    try:
        query = f"SELECT COUNT(*) FROM `{table_id}` "
        query += f"WHERE DATE = '{check_date.strftime('%Y-%m-%d')}'"
        result = client.query(query).result()
        for row in result:
            return row[0] > 0
    except BadRequest as e:
        logging.error("Error in querying table %s: %s", table_id, e)
        return False


def delete_data_for_date_range(client, table_id, start_date_del, end_date_del):
    """
    Delete data from a BigQuery table for a specified date range
    """
    start_str = start_date_del.strftime("%Y-%m-%d")
    end_str = end_date_del.strftime("%Y-%m-%d")

    delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE SAFE_CAST(DATE AS DATE) BETWEEN "{start_str}" AND "{end_str}"
                    """
    client.query(delete_query).result()
    logging.info(
        "Deleted data from %s to %s in table %s", start_str, end_str, table_id
    )


MAX_RETRIES = 3  # Maximum number of retries
RETRY_DELAY = 60  # Delay between retries in seconds


def load_data_for_date_range(
    query_id_load,
    start_date_load,
    end_date_load,
    table_suffix_load,
    skip_deletion=False,
):
    """
    Run the report for the specified date range and load the data to BigQuery
    """
    project_id = config["project_id"]
    dataset_id = config["dataset_id"]
    create_bigquery_dataset_if_not_exists(bq_client, project_id, dataset_id)

    table_id = f"{project_id}.{dataset_id}.{table_suffix_load}"

    # Check if the table exists once at the beginning
    table_already_exists = table_exists(bq_client, table_id)

    # If skip_deletion is False, and the table exists, delete data for the specified date range
    if not skip_deletion and table_exists(bq_client, table_id):
        delete_data_for_date_range(
            bq_client, table_id, start_date_load, end_date_load
        )

    current_date = start_date_load
    while current_date <= end_date_load:
        logging.info(
            "Processing data for date: %s", current_date.strftime("%Y-%m-%d")
        )
        attempts = 0

        while attempts < MAX_RETRIES:
            try:
                if not data_already_loaded(bq_client, table_id, current_date):
                    dataframe = run_report(
                        gam_client,
                        report_service,
                        config["api_version"],
                        query_id_load,
                        current_date.year,
                        current_date.month,
                        current_date.day,
                        current_date.year,
                        current_date.month,
                        current_date.day,
                    )

                    if not dataframe.empty:
                        if (
                            not table_already_exists
                        ):  # Create table if it doesn't exist
                            create_bigquery_table_if_not_exists(
                                bq_client, table_id, dataframe
                            )
                            table_already_exists = (
                                True  # Update flag as table now exists
                            )

                        job_config = bigquery.LoadJobConfig(
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                            autodetect=True,
                        )
                        bq_client.load_table_from_dataframe(
                            dataframe, table_id, job_config=job_config
                        ).result()
                        logging.info(
                            "Data for %s loaded to %s",
                            current_date.strftime("%Y-%m-%d"),
                            table_id,
                        )

                    logging.info(
                        "No data to load for %s",
                        current_date.strftime("%Y-%m-%d"),
                    )
                    break

                logging.info(
                    "Data for %s already loaded. Skipping.",
                    current_date.strftime("%Y-%m-%d"),
                )
                break
            except Exception as e:  # pylint: disable=broad-except
                logging.error(
                    "Attempt %s failed for date %s: %s",
                    attempts + 1,
                    current_date.strftime("%Y-%m-%d"),
                    e,
                )
                attempts += 1
                if attempts < MAX_RETRIES:
                    logging.info("Retrying in %s seconds...", RETRY_DELAY)
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(
                        "Max retries reached for date %s. Skipping to next date.",
                        current_date.strftime("%Y-%m-%d"),
                    )
                    break

        current_date += timedelta(days=1)


def load_data_for_date_range_validated(
    query_id_load_val,
    start_date_load_val,
    end_date_load_val,
    table_suffix_load_val,
    skip_deletion=False,
    pydantic_data_model=None,
):  # pylint: disable=too-many-arguments
    """
    run the report for the specified date range and load the data to BigQuery
    """
    project_id = config["project_id"]
    dataset_id = config["dataset_id"]

    create_bigquery_dataset_if_not_exists(bq_client, project_id, dataset_id)

    table_id = f"{project_id}.{dataset_id}.{table_suffix_load_val}"
    bq_schema = pydantic_model_to_bq_schema(pydantic_data_model)

    # If skip_deletion is False, and the table exists, delete data for the specified date range
    if not skip_deletion and table_exists(bq_client, table_id):
        delete_data_for_date_range(
            bq_client, table_id, start_date_load_val, end_date_load_val
        )

    current_date = start_date_load_val
    while current_date <= end_date_load_val:
        attempts = 0

        while attempts < MAX_RETRIES:
            try:
                if not data_already_loaded(bq_client, table_id, current_date):
                    logging.info("Loading data for date: %s", current_date)
                    datalist = run_report_validated(
                        gam_client,
                        report_service,
                        config["api_version"],
                        query_id_load_val,
                        current_date.year,
                        current_date.month,
                        current_date.day,
                        current_date.year,
                        current_date.month,
                        current_date.day,
                        pydantic_data_model=pydantic_data_model,
                    )

                    if datalist:
                        # Check if the table exists once at the beginning
                        table_already_exists = table_exists(
                            bq_client, table_id
                        )

                        if table_already_exists:
                            logging.info("Table %s already exists.", table_id)
                        elif not table_already_exists:
                            logging.info("Table %s does not exist.", table_id)
                            ctie_bq(
                                bq_client,
                                dataset_id,
                                table_suffix_load_val,
                                bq_schema,
                                partition_field="DATE",
                            )
                            logging.info("Table %s created.", table_id)

                        load_data_to_bigquery_append(
                            client=bq_client,
                            dataset_id=dataset_id,
                            table_id=table_suffix_load_val,
                            records=datalist,
                            schema=bq_schema,
                        )

                        logging.info(
                            "Data for %s loaded to %s",
                            current_date.strftime("%Y-%m-%d"),
                            table_id,
                        )

                    logging.info(
                        "No data to load for %s",
                        current_date.strftime("%Y-%m-%d"),
                    )
                    break

                logging.info(
                    "Data for %s already loaded. Skipping.",
                    current_date.strftime("%Y-%m-%d"),
                )
                break
            except Exception as e:  # pylint: disable=broad-except
                logging.error(
                    "Attempt %s failed for date %s: %s",
                    (attempts + 1),
                    current_date.strftime("%Y-%m-%d"),
                    e,
                )
                attempts += 1
                if attempts < MAX_RETRIES:
                    logging.info("Retrying in %s seconds...", RETRY_DELAY)
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(
                        "Max retries reached for date %s. Skipping to next date.",
                        current_date.strftime("%Y-%m-%d"),
                    )
                    break

        current_date += timedelta(days=1)


# For directly running this script, and testing
if __name__ == "__main__":
    QUERY_ID = "default_query_id"
    start_date = datetime.today() - timedelta(days=10)  # Example start date
    end_date = datetime.today()  # Example end date
    TABLE_SUFFIX = "default_table"
    load_data_for_date_range(QUERY_ID, start_date, end_date, TABLE_SUFFIX)
