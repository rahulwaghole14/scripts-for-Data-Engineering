""" create bigquery table """
import logging
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning


def create_bigquery_table(client, dataset_id, table_id, schema):
    """create bigquery table or update schema if table already exists"""
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    table = bigquery.Table(table_ref, schema=schema)

    # Set partitioning options for date_at field by day
    time_partitioning = TimePartitioning(field="date_at")
    time_partitioning.type_ = "DAY"
    table.time_partitioning = time_partitioning

    try:
        table = client.create_table(table)  # API request
        logging.info(
            f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
        )
        return table
    except Exception as error:
        logging.info(error)
        # Table already exists, drop it and create a new one with updated schema
        logging.info(f"Table {table_id} already exists. Checking schema.")
        # if table schema is different, drop and recreate
        if table.schema != schema:
            logging.info(
                f"Table {table_id} schema is different. Dropping and recreating."
            )
            try:
                client.delete_table(table_ref)  # API request
                logging.info(f"Table {table_id} deleted.")
            except Exception as e:
                pass

            # Create new table with updated schema and partitioning
            table = bigquery.Table(table_ref, schema=schema)
            time_partitioning = TimePartitioning(field="date_at")
            time_partitioning.type_ = "DAY"
            table.time_partitioning = time_partitioning
            table = client.create_table(table)  # API request
            logging.info(
                f"Table {table_id} created with updated schema and partitioning."
            )
            return table
        else:
            logging.info(f"Table {table_id} schema is already up to date.")
            return table
