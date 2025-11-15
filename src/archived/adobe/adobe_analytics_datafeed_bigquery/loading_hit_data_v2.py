"""
Module: bigquery_loader.py

This module provides utilities to load hit data into BigQuery.

Functions:
- clean_column_name: Cleans column names to make them compliant with BigQuery naming conventions.
- load_hit_data: Loads data into BigQuery from a specified Google Cloud Storage bucket.

"""

import logging
import re
import io
import time
import os
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import (
    NotFound,
    BadRequest,
    Forbidden,
    InternalServerError,
    TooManyRequests,
)
from logger import logger

filename = os.path.splitext(os.path.basename(__file__))[0]
logger_name = f"adobe_analytics_{filename}"
logger(logger_name)


def clean_column_name(column: str) -> str:
    """
    Cleans the column name to make it compliant with BigQuery naming conventions.

    Args:
        column (str): The original column name.

    Returns:
        str: The cleaned column name.
    """
    column = re.sub("[^0-9a-zA-Z]+", "_", column)
    column = re.sub("_+", "_", column)
    return column


def load_hit_data(
    bucket_name: str, header_blob_name: str, data_blob_name: str, table_id: str
) -> None:
    """
    Loads hit data into BigQuery from the specified Google Cloud Storage bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        header_blob_name (str): The blob name where the header data is stored.
        data_blob_name (str): The blob name where the main data is stored.
        table_id (str): The BigQuery table ID where the data will be loaded.

    Returns:
        None
    """
    start_time = time.perf_counter()

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    bucket = storage_client.bucket(bucket_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
        allow_jagged_rows=True,
    )
    job_config.max_bad_records = 1000000

    try:
        header_blob = storage.Blob(header_blob_name, bucket)
        header_line = header_blob.download_as_text().strip()
        column_headers = [
            clean_column_name(header) for header in header_line.split("\t")
        ]
    except Exception as error:
        column_headers = []
        logging.error("Error reading %s: %s", header_blob_name, str(error))

    data_blob = storage.Blob(data_blob_name, bucket)
    data_stream = io.BytesIO(data_blob.download_as_bytes())
    chunk_size = 10000
    chunk_counter = 0

    for chunk in pd.read_csv(
        data_stream,
        delimiter="\t",
        names=column_headers,
        header=None,
        encoding="UTF-8",
        chunksize=chunk_size,
        low_memory=False,
        dtype=str,  # This line ensures all data is loaded as strings
    ):
        chunk = chunk.astype(str)
        chunk.fillna("", inplace=True)
        chunk_counter += 1
        logging.info("Processing chunk %s...", chunk_counter)
        try:
            job = bigquery_client.load_table_from_dataframe(
                chunk, table_id, job_config=job_config, timeout=150
            )
            job.result()
        except (
            BadRequest,
            Forbidden,
            NotFound,
            TooManyRequests,
            InternalServerError,
        ) as e:
            logging.info("Error: %s", e)
        except ValueError as e:
            problematic_data = str(e).split("'")[
                1
            ]  # Extracting the problematic data from the error message
            problematic_columns = chunk.columns[
                chunk.isin([problematic_data]).any()
            ].tolist()  # Finding the columns that have the problematic data
            logging.info(
                "ValueError with data '%s' found in columns: %s",
                problematic_data,
                problematic_columns,
            )
        except Exception as error:
            logging.info("Unexpected error: %s", error)

    logging.info("Data added")
    end_time = time.perf_counter()
    duration = end_time - start_time
    logging.info("Duration: %s seconds", duration)
