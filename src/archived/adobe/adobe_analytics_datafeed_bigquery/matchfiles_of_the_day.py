"""
This module is designed to demonstrate the usage of logging.
It provides functionality to log information, warnings, and errors.
"""

import logging
import os
import time
from dotenv import load_dotenv
from google.cloud import storage
from logger import logger

# Setup Logging
filename = os.path.splitext(os.path.basename(__file__))[0]
logger_name = f"adobe_analytics_{filename}"
logger(logger_name)

load_dotenv()


project_id = os.getenv("project_id")


def get_matching_filenames(
    bucket_name: str,
    time_stamp: str,
    report_name: str,
) -> list:
    """
    Return the filenames from the GCS bucket that match a specific prefix and end with '.tar.gz'.

    Args:
    - bucket_name (str): Name of the GCS bucket.
    - timestamp (str): The timestamp prefix to match filenames against.

    Returns:
    - List of matched filenames or empty list.
    """
    start_time = time.time()
    # Initialize the GCS client
    storage_client = storage.Client()

    # Form the prefix to filter the desired files
    file_prefix = f"{report_name}_{time_stamp}"
    # List all blobs with the given prefix
    blobs = storage_client.list_blobs(bucket_name, prefix=file_prefix)

    matched_files = []
    for blob in blobs:
        if blob.name.endswith(".tar.gz"):
            matched_files.append(blob.name)

    matched_files_count = len(matched_files)
    logging.info("Found %d matching files.", matched_files_count)
    if matched_files_count:
        for file in matched_files:
            logging.info(file)
    else:
        logging.warning("No matching files found.")
    elapsed_time = time.time() - start_time
    logging.info(
        "Elapsed Time for matchfiles of the day: %.2f seconds", elapsed_time
    )

    return matched_files
