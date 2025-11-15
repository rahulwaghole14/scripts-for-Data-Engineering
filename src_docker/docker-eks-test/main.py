"""This module does something related to Google BigQuery and AWS secrets."""

import logging
import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from aws_secret import get_secret
from logging_config import configure_logging


# Configure logging
configure_logging()

# Define the path to the config file relative to the script's location
# Gets the directory where the script is located


project_id = "hexa-data-report-etl-prod"
DATASET_ID = "docker_test"
TABLE_ID = "test_table_1"
CSV_FILE_PATH = "oval_office_subscribers_matched.csv"
REGION_NAME = "ap-southeast-2"

# Add an additional blank line before the function definition


# Example usage
try:
    # Specify your AWS credentials and secret details here

    # Fetch the secret
    secret_str = get_secret()
    google_creds_json = json.loads(secret_str)

    # Initialize the BigQuery client with the fetched credentials
    credentials = service_account.Credentials.from_service_account_info(
        google_creds_json
    )
    client = bigquery.Client(
        credentials=credentials, project=google_creds_json["project_id"]
    )

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assumes the first row is the header
        autodetect=True,  # Auto-detect schema
        # Allow for missing values in the last column(s)
        allow_jagged_rows=True,
        ignore_unknown_values=True,  # Ignore extra, unknown values
        # in the CSV data
        schema_update_options=[  # Allows schema changes during the load
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
        max_bad_records=10,  # Process up to 10 bad records before failing
        field_delimiter=",",
        null_marker=None,  # Specify a string that represents a NULL value
        quote_character='"',  # Specify the quote character,
        # double quote is the default
    )

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    script_dir_csv = os.path.dirname(__file__)
    # Creates the full path to the config file
    config_path_csv = os.path.join(
        script_dir_csv, "oval_office_subscribers_matched.csv"
    )

    # Load the CSV data into BigQuery
    with open(config_path_csv, "rb") as config_path_csv:
        job = client.load_table_from_file(
            config_path_csv,
            table_ref,
            location="australia-southeast1",  # Specify BQ dataset location
            job_config=job_config,
        )

    # Waits for the job to complete
    job.result()

    print(
        f"The file {CSV_FILE_PATH} has been uploaded to "
        f"{DATASET_ID}.{TABLE_ID}"
    )
    # send_log_to_cloudwatch(
    #     f"The file {CSV_FILE_PATH} has been uploaded to "
    #     f"{DATASET_ID}.{TABLE_ID}"
    # )
    # send_log_to_cloudwatch("File uploaded to BigQuery successfully")

except json.JSONDecodeError as e:
    logging.info("JSON decode error: %s", e)
    # send_log_to_cloudwatch("JSON decode error: %s", e)
    # publish_message_to_sns(
    #     "arn:aws:sns:ap-southeast-2:190748537425:test",
    #     "JSON decode error: %s" % e,
    # )
