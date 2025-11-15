""" append dataframe to bigquery """
import logging

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def append_dataframe_to_bigquery(data_frame, client, table_id, schema):
    """to append the transformed or processed breakdown dataframe to BigQuery table"""

    if not data_frame.empty:
        try:
            table = client.get_table(table_id)
            logging.info(
                "Table '%s' already exists. has %s rows",
                table_id,
                table.num_rows,
            )
            # logging.info("Appending dataframe for {} to the existing table".format(yesterday))
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = client.load_table_from_dataframe(
                data_frame, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
        except NotFound:
            logging.info("Table '%s' is not found.", table_id)
            # Create a new table
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)
            # Append dataframe to an empty table
            logging.info("Appending DataFrame to an empty table")
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = client.load_table_from_dataframe(
                data_frame, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
    else:
        logging.info("No dataframe to append")
