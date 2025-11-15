from tkinter.font import names
from google.oauth2 import service_account
from google.cloud import bigquery
from .create_uuid import generate_uuid

import os
import json
import logging
import datetime
from common.aws.aws_secret import get_secret


def update_marketing_ids():
    # Initialize logging
    # log_filename = os.path.join(
    #     os.getcwd(), "logs", f"PressPatron_{datetime.date.today()}.log"
    # )
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    #     handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
    # )

    # Load environment variables from secrets
    data_presspatron = get_secret("datateam_presspatron")
    data_presspatron_json = json.loads(data_presspatron)

    project_id = data_presspatron_json["project_id"]
    dataset_id = data_presspatron_json["dataset_id"]
    table_name = data_presspatron_json["table_name"]
    UUID_NAMESPACE = data_presspatron_json["UUID_NAMESPACE"]

    bq_sec = get_secret("datateam_google_prod")
    bigquery_secret = json.loads(bq_sec)

    credentials = service_account.Credentials.from_service_account_info(
        bigquery_secret
    )
    client = bigquery.Client(
        credentials=credentials, project=bigquery_secret["project_id"]
    )

    namespace_uuid = UUID_NAMESPACE

    # Query to retrieve user IDs or emails from BigQuery
    query = f"""
    SELECT user_id, LOWER(TRIM(email)) AS email
    FROM `{project_id}.{dataset_id}.{table_name}`
    WHERE marketing_id IS NULL
    """

    results = client.query(query).result().to_dataframe()

    # Check if there are records to update
    if results.empty:
        logging.info("No records to update for marketing_id. Exiting script.")
        return

    # Generate UUID version 5
    results["marketing_id"] = results.apply(
        lambda x: generate_uuid(x["email"], namespace_uuid), axis=1
    ).astype(str)

    # Create a new dataframe with just the user_id and the new marketing_id
    update_df = results[["user_id", "marketing_id"]]

    # Define the destination table for the temporary data
    temporary_table_name = (
        f"{project_id}.{dataset_id}.temporary_table_for_uuids"
    )

    # Create a temporary table
    create_temp_table_query = f"""
    CREATE OR REPLACE TABLE `{temporary_table_name}` AS
    SELECT CAST(NULL AS STRING) AS user_id, CAST(NULL AS STRING) AS marketing_id
    """

    # Run the create table query
    create_temp_table_job = client.query(create_temp_table_query)
    create_temp_table_job.result()

    # Load the data into the temporary table
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(
        update_df, temporary_table_name, job_config=job_config
    )
    load_job.result()

    # Merge query to update marketing_id
    merge_query = f"""
    MERGE `{project_id}.{dataset_id}.{table_name}` AS target
    USING `{temporary_table_name}` AS source
    ON target.user_id = source.user_id
    WHEN MATCHED THEN
      UPDATE SET target.marketing_id = source.marketing_id
    """

    # Execute the merge query
    merge_job = client.query(merge_query)
    merge_job.result()

    # Track how many rows have been updated
    rows_updated = merge_job.num_dml_affected_rows
    logging.info(
        f"Process complete. Number of rows updated with marketing_id: {rows_updated}"
    )

    # Optionally, delete the temporary table
    client.delete_table(temporary_table_name, not_found_ok=True)
