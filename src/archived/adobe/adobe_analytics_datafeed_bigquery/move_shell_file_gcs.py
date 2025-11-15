import os
import logging
from google.cloud import storage
from logger import logger
from dotenv import load_dotenv

filename = os.path.splitext(os.path.basename(__file__))[0]
logger_name = f"adobe_analytics_{filename}"
logger(logger_name)

load_dotenv()


def upload_to_bucket(blob_name, filename, bucket_name):
    """Upload data to a bucket"""

    # Initialize a storage client
    storage_client = storage.Client()

    # Construct the full local path to the file
    file_path = os.path.join(os.getcwd(), filename)

    # Make sure the file exists
    if not os.path.isfile(file_path):
        print(f"The file {filename} does not exist in the current directory.")
        return

    # Get the bucket object
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob object from the filepath
    blob = bucket.blob(blob_name)

    # Upload the file
    blob.upload_from_filename(file_path)

    print(f"File {file_path} uploaded to {blob_name}.")


# The name of your GCS bucket
bucket_name = os.environ.get("bucket_name")

# The name of the file in your current working directory
filename = "src/adobe_analytics_datafeed_bigquery/script.sh"

# The desired object name (including any folder structure) in GCS
blob_name = "scripts/script.sh"

upload_to_bucket(blob_name, filename, bucket_name)
