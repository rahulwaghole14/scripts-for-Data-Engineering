""" get existing dates from table """
import logging


def get_dates(client, table_id):
    """get dates from bigquery table"""
    try:
        table = client.get_table(table_id)
        logging.info("Table '%s' exists.", table_id)
        query = f"SELECT DISTINCT date_at FROM `{table_id}`"

        data = client.query(query)
        # logging.info(f"[{account}] Table '{table_id}' has {table.num_rows} rows for {account}")
        date_list = []
        for row in data:
            date_list.append(row[0])
        logging.info("dates exist in bigquery table: %s", len(date_list))
        return date_list
    except Exception as error:
        logging.info("Table '%s' is not found. %s", table_id, error)
        # Returning empty date list and run the queries for 1 year
        return []
