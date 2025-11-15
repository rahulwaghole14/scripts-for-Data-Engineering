"""
This module contains a function, `move_blob`,
for moving a blob within the same GCS (Google Cloud Storage) bucket.

Usage:
    Suitable for reorganizing blobs within a GCS bucket,
    e.g., moving processed files to a different directory.
"""

import os
import logging
from google.cloud import storage
from logger import logger

filename = os.path.splitext(os.path.basename(__file__))[0]
logger_name = f"adobe_analytics_{filename}"
logger(logger_name)


def move_blob(bucket_name, blob_name, new_blob_name):
    """
    Move a blob from one directory to another within a GCS bucket.

    Parameters:
        bucket_name (str): The name of the GCS bucket.
        blob_name (str): The name (path) of the blob that needs to be moved.
        new_blob_name (str): The new name (path) of the blob after moving.

    Returns:
        None. The function performs the move operation and logs the status.

    Usage:
        move_blob('my_bucket', 'path/to/blob', 'path/to/new_blob')
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    logging.info("Processing blob: %s", blob_name)

    new_blob = bucket.copy_blob(blob, bucket, new_blob_name)
    logging.info("Blob copied to new location: %s", new_blob_name)

    if new_blob.exists():
        logging.info(
            "Blob %s copied to %s within %s bucket.",
            blob_name,
            new_blob_name,
            bucket_name,
        )
        blob.delete()
        logging.info("Deleted the original blob %s.", blob_name)
    else:
        logging.info("Failed to move %s to %s.", blob_name, new_blob_name)
