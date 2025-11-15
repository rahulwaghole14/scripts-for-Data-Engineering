"""
Adobe Analytics Data Processing

This module processes data from Adobe Analytics. It fetches matching files for the
specified day, organizes them into lookup tables, and loads them into the appropriate
buckets and datasets. The primary function 'main' orchestrates the flow of data from
source to destination.

Modules:
- matchfiles_of_the_day: Matches files based on the current day.
- create_directory: Handles directory creation in the bucket.
- extract_file_to_gcs_v2: Streams data between source and destination buckets.
- load_lookup: Loads specific tables.
- loading_hit_data_v2: Loads the primary hit data.
- move_tarfile_to_folder: Handles movement of tar files.

Environment variables are used to specify bucket details, dataset information, and report names.

Note: Ensure necessary environment variables are set before running this module.
"""
# Standard library imports
import logging
import os
import time
from datetime import datetime

# Third-party imports
from dotenv import load_dotenv

# Application-specific imports
import create_directory
import extract_file_to_gcs_v2
import load_lookup
import loading_hit_data_v2
import matchfiles_of_the_day
import move_tarfile_to_folder
from logger import logger


load_dotenv()

if load_dotenv():
    print(".env file loaded successfully")
else:
    print(".env file not loaded")

# Setup Logging
filename = os.path.splitext(os.path.basename(__file__))[0]
logger_name = f"adobe_analytics_{filename}"
logger(logger_name)

# Function implementation
bucket_name = os.environ.get("bucket_name")
source_bucket_name = bucket_name
dest_bucket_name = bucket_name
move_folder = os.environ.get("move_folder")
source_folder = os.environ.get("source_folder")
dataset_id = os.environ.get("dataset_id")
report_name = os.environ.get("report_name")


def main():
    """
    This is the main function that will run when
    this script is executed. It handles the
    initialization and starts the program.
    """

    start_time = time.time()
    logging.info("Program started at %s", start_time)

    timestamp = datetime.now().strftime("%Y%m%d")
    # timestamp = "20231009-06"
    lookup_tables = [
        "browser",
        "browser_type",
        "color_depth",
        "connection_type",
        "country",
        "event",
        "javascript_version",
        "languages",
        "operating_systems",
        "plugins",
        "resolution",
        "search_engines",
    ]

    matched_files = matchfiles_of_the_day.get_matching_filenames(
        bucket_name, timestamp, report_name
    )
    for match in matched_files:
        file_name = match[:-7]
        logging.info(file_name)
        directory_path = f"{source_folder}/{file_name}"
        data_blob_name = f"{directory_path}/hit_data.tsv"
        header_blob_name = f"{directory_path}/column_headers.tsv"
        table_id = f"{dataset_id}.hit_data_final_{timestamp}"
        blob_name = match  # The original file path in the GCS bucket
        new_blob_name = f"{move_folder}/{match}"
        create_directory.create_directory_in_bucket(
            bucket_name, directory_path
        )
        extract_file_to_gcs_v2.stream_gcs_to_gcs(
            source_bucket_name, match, dest_bucket_name, directory_path
        )
        for table in lookup_tables:
            load_lookup.load_table(
                bucket_name, directory_path, table, dataset_id
            )
        loading_hit_data_v2.load_hit_data(
            bucket_name, header_blob_name, data_blob_name, table_id
        )
        move_tarfile_to_folder.move_blob(bucket_name, blob_name, new_blob_name)
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        logging.info(
            "Elapsed Time for main program: %d minutes, %.2f seconds",
            minutes,
            seconds,
        )

        logging.info(matched_files)


if __name__ == "__main__":
    main()
