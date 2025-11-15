"""get dates from bigquery"""
import logging
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
        table = client.get_table(table_id)
        logging.info("Table %s exists. Preparing to run query.", table)

        query = f"SELECT DISTINCT date FROM `{table_id}` WHERE brightcove_account = '{account}'"
        data = client.query(query)

        for row in data:
            date_list.append(row[0])

        logging.info(
            "Successfully retrieved %s distinct dates for account %s in table %s.",
            len(date_list),
            account,
            table_id,
        )

    except NotFound:
        logging.warning("Table %s is not found.", table_id)

    except GoogleAPIError as e:
        logging.error("Error while querying table: %s", str(e))

    except Exception as error:  # pylint: disable=broad-except
        logging.critical(
            "An unexpected error occurred in get_dates(): %s", error
        )

    return date_list
