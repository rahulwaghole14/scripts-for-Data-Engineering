"""
This module is used to extract file from one gsc folder to another.
It extarcts .tar.gz file into the individual .tsv files which can be
"""

import io
import tarfile
import logging
from retrying import retry
from google.cloud import storage
from dotenv import load_dotenv
from logger import logger

logger("adobe_analytics")

load_dotenv()


@retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=60000,
    stop_max_attempt_number=10,
)
def upload_blob_with_retry(dest_blob, buffer, size):
    """
    Uploads a blob to a destination with retry on failure.

    Args:
        dest_blob: The destination blob to upload.
        buffer: The buffer containing the content to be uploaded.
        size: The size of the content to be uploaded.
    """
    buffer.seek(0)  # Reset buffer to the beginning
    dest_blob.upload_from_file(buffer, size=size)


def stream_gcs_to_gcs(
    source_bucket_name,
    source_blob_name,
    dest_bucket_name,
    dest_folder_in_gcs,
):
    """
    Streams content from a blob in one GCS bucket to another GCS bucket.

    Args:
        source_bucket_name: The name of the source bucket.
        source_blob_name: The name of the source blob.
        dest_bucket_name: The name of the destination bucket.
        dest_folder_in_gcs: The name of the folder in the destination bucket.
        chunk_size: The size of the chunk to be read and uploaded at once.
    """
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    in_memory_file = io.BytesIO()

    source_blob.download_to_file(in_memory_file)
    in_memory_file.seek(0)

    with tarfile.open(fileobj=in_memory_file, mode="r|gz") as tar:
        for member in tar:
            if member.isfile():
                dest_blob_path = f"{dest_folder_in_gcs}/{member.name}"
                dest_bucket = storage_client.bucket(dest_bucket_name)
                dest_blob = dest_bucket.blob(dest_blob_path)

                with tar.extractfile(member) as file_obj:
                    try:
                        content = file_obj.read().decode(
                            "utf-8", errors="replace"
                        )
                        buffer = io.BytesIO(content.encode("utf-8"))
                        upload_blob_with_retry(dest_blob, buffer, len(content))
                        logging.info(
                            "Uploaded %s to %s in GCS.",
                            member.name,
                            dest_blob_path,
                        )
                    except Exception as error:
                        logging.info(
                            "Error occurred while processing %s: %s",
                            member.name,
                            error,
                        )
                        continue  # Skip to the next file on error
