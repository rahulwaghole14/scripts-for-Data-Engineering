# pylint: disable=all

from google.cloud import bigquery

# from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from .logging_module import logger

logger()


def append_dataframe_to_bigquery(df, client, table_id, bq_schema, date):
    """
    Append dataframe to BigQuery.

    :param df: The dataframe to append
    :type df: Pandas dataframe

    :param table_id: The ID of the BigQuery table
    :type table_id: string

    :param client: BigQuery client object
    :type client: object

    :param bq_schema: Specifying BigQuery schema
    :type bq_schema: list

    :param date: Adding extraction date in the log file
    :type date: datetime64

    :returns: None
    """
    if not df.empty:
        try:
            table = client.get_table(table_id)
            # logging.info("Table '{}' already exists.".format(table_id))
            # logging.info("Table '{}' has {} rows".format(table_id, table.num_rows))
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logging.info(
                "Appending DataFrame with {} rows to {}@ {}".format(
                    df.shape[0], table_id, date
                )
            )

        except NotFound:
            logging.info("Table '{}' is not found.".format(table_id))
            # Create a new table
            table = bigquery.Table(table_id, schema=bq_schema)
            table = client.create_table(table)
            # Append dataframe to an empty table
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logging.info(
                "Appending DataFrame with {} rows to a new table: {}@ {}".format(
                    df.shape[0], table_id, date
                )
            )

    else:
        logging.info("No dataframe to append")


def overwrite_dataframe_to_bigquery(df, client, table_id, bq_schema, date):
    """
    Overwrite dataframe to BigQuery.

    :param df: The dataframe to overwrite
    :type df: Pandas dataframe

    :param table_id: The ID of the BigQuery table
    :type table_id: string

    :param client: BigQuery client object
    :type client: object

    :param bq_schema: Specifying BigQuery schema
    :type bq_schema: list

    :param date: Adding extraction date in the log file
    :type date: datetime64

    :returns: None
    """
    if not df.empty:
        try:
            table = client.get_table(table_id)
            job_config = bigquery.LoadJobConfig(
                schema=bq_schema,
                write_disposition="WRITE_TRUNCATE",
            )

            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logging.info(
                "Overwriting DataFrame with {} rows to {}@ {}".format(
                    df.shape[0], table_id, date
                )
            )
        except NotFound:
            logging.info("Table '{}' is not found.".format(table_id))
            # Create a new table
            table = bigquery.Table(table_id, schema=bq_schema)
            table = client.create_table(table)
            # Append dataframe to an empty table
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            job_config.allow_quoted_newlines = True
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            logging.info(
                "Appending DataFrame with {} rows to new table: {}@ {}".format(
                    df.shape[0], table_id, date
                )
            )
    else:
        logging.info("No dataframe to append")
