"""Get dates from BigQuery"""
import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.cloud.exceptions import NotFound


def get_dates(client, table_id, account):
    """
    Retrieve distinct dates from a BigQuery table for a specific account.

    :param client: The BigQuery client.
    :param table_id: The ID of the BigQuery table.
    :param account: The account to filter by.
    :return: A list of distinct dates for the specified account.
    """

    date_list = []

    try:
        # Ensure the table exists
        client.get_table(
            table_id
        )  # This will raise NotFound if table does not exist
        logging.info("Table %s exists. Preparing to run query.", table_id)

        # Safely prepare and run the parameterized query
        query = """
            SELECT DISTINCT date
            FROM `{}`
            WHERE brightcove_account = @account
        """.format(
            table_id
        )  # Direct table ID insertion as it can't be parameterized
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("account", "STRING", account)
            ]
        )
        query_job = client.query(
            query, job_config=job_config
        )  # Safe query execution

        # Fetch the results
        for row in query_job:
            date_list.append(
                row.date
            )  # Assuming 'date' is the field name in the row

        logging.info(
            "Successfully retrieved %s distinct dates for account %s in table %s.",
            len(date_list),
            account,
            table_id,
        )

    except NotFound:
        logging.warning("Table %s not found.", table_id)

    except GoogleAPIError as e:
        logging.error("Error while querying table: %s", e)

    except Exception as error:  # Using a broad except can mask unexpected errors
        logging.critical(
            "An unexpected error occurred in get_dates(): %s", error
        )

    return date_list
