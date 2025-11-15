"""
This module is designed to demonstrate the usage of logging.
It provides functionality to log information, warnings, and errors.
"""
import logging
import time
from google.cloud import storage


from logger import logger

logger("adobe_analytics")


def create_directory_in_bucket(bucket_name, directory_path):
    """
    Create a directory-like structure in a GCS bucket.

    Args:
    - bucket_name (str): The name of the GCS bucket.
    - directory_path (str): The directory path to create, e.g., "mydir/subdir/".
                            Make sure it ends with a slash.
    """
    start_time = time.time()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Check if directory_path ends with a slash and add one if not
    if not directory_path.endswith("/"):
        directory_path += "/"

    # Create an empty blob with the directory name
    blob = bucket.blob(directory_path)
    blob.upload_from_string("")

    logging.info("Directory %s created in %s.", directory_path, bucket_name)
    elapsed_time = time.time() - start_time
    logging.info(
        "Elapsed Time for create_directory: %.2f seconds", elapsed_time
    )
