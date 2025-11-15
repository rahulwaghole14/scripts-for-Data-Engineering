import os
import json
import boto3
import logging
import zipfile
import pytz
import pandas as pd
import csv
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from common.bigquery.bigquery import (
    load_csv_to_bigquery,
)
from common.aws.aws_secret import get_secret
from common.aws.aws_s3 import list_files_in_s3_bucket
from botocore.exceptions import (
    NoCredentialsError,
    PartialCredentialsError,
    ClientError,
)
from .secrets import (
    project_id,
    dataset_id_q,
    table_id_q,
    client,
    dataset_id,
    bucket_name,
    aws_access_key_id,
    aws_secret_access_key,
    region_name,
    prefix,
)

logging.basicConfig(level=logging.INFO)


def process_zip_file(
    file,
    bucket_name,
    aws_access_key_id,
    aws_secret_access_key,
    region_name,
    client,
    dataset_id,
):
    """
    Downloads a zip file from S3, unzips it, converts .txt to .csv (comma-separated), and deletes the zip file.

    Args:
        file (str): The path of the zip file in the S3 bucket.
        bucket_name (str): The S3 bucket name.
        aws_access_key_id (str): AWS access key ID.
        aws_secret_access_key (str): AWS secret access key.
        region_name (str): AWS region.
        client (bigquery.Client): BigQuery client for loading data.
        dataset_id (str): BigQuery dataset ID where tables are located.
    """
    try:
        # Initialize the S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        # Download the zip file
        local_zip_file = file.split("/")[
            -1
        ]  # Extracting the filename from the path
        s3.download_file(bucket_name, file, local_zip_file)

        # Unzip the file
        with zipfile.ZipFile(local_zip_file, "r") as zip_ref:
            extract_folder = local_zip_file.split(".")[
                0
            ]  # Create a folder with the same name as the zip file (without extension)
            zip_ref.extractall(extract_folder)
            logging.info(f"Extracted {local_zip_file} to {extract_folder}")

        # Delete the zip file after extraction
        os.remove(local_zip_file)
        logging.info(f"Deleted zip file: {local_zip_file}")

        # List the files in the unzipped folder and convert .txt files to .csv
        logging.info(f"Files in {extract_folder}:")
        for extracted_file in os.listdir(extract_folder):
            if extracted_file.endswith(".txt"):
                txt_file_path = os.path.join(extract_folder, extracted_file)
                csv_file_path = os.path.join(
                    extract_folder, extracted_file.replace(".txt", ".csv")
                )
                convert_txt_to_csv(txt_file_path, csv_file_path)
                load_csv_to_bigquery(client, dataset_id, csv_file_path)

    except Exception as e:
        logging.info(f"An error occurred while processing {file}: {e}")


def convert_txt_to_csv(txt_file_path, csv_file_path=None):
    """
    Converts a .txt file to a .csv file with the following format:
    - Comma (`,`) as the delimiter
    - Double quotes (`"`) to quote fields
    - The first row is used as the header.
    - Adds a new column with a timestamp and ensures spaces in filenames are replaced with underscores.

    Args:
        txt_file_path (str): The path to the input .txt file.
        csv_file_path (str, optional): The path to the output .csv file. If not provided, it generates a new filename.
    """
    try:
        # Generate a new CSV filename if not provided
        if csv_file_path is None:
            base_name = os.path.splitext(txt_file_path)[
                0
            ]  # Get the filename without extension
            base_name = base_name.replace(
                " ", "_"
            )  # Replace all spaces with underscores
            csv_file_path = (
                f"{base_name}.csv"  # Use the base name with a .csv extension
            )

        # Replace spaces in the csv file path (if any were missed)
        csv_file_path = csv_file_path.replace(" ", "_")

        # Read the .txt file with pipe delimiter and double quotes
        df = pd.read_csv(
            txt_file_path, delimiter="|", quotechar='"', dtype=str
        )  # Treat all columns as strings by default

        # Replace spaces in the column names with underscores
        df.columns = [col.replace(" ", "_") for col in df.columns]

        # Set the timezone
        timezone = pytz.timezone("Pacific/Auckland")

        # Add a new column with the current timestamp to the DataFrame
        timestamp = datetime.now(timezone).strftime(
            "%Y-%m-%d %H:%M:%S"
        )  # Current timestamp
        yyyymm = csv_file_path.replace(".csv", "").split("_")[-1]
        df["Ingestion_time"] = timestamp  # Add new column to DataFrame
        df[
            "YYYYMM"
        ] = yyyymm  # Add new column with 'YYYYMM' extracted from the filename

        # Save the dataframe to a .csv file with commas as the delimiter
        df.to_csv(
            csv_file_path, index=False, sep=",", quoting=csv.QUOTE_ALL
        )  # Comma-separated with double quotes for quoting
        logging.info(
            f"Converted {txt_file_path} to {csv_file_path} with added timestamp column."
        )

        # Delete the original .txt file after conversion
        os.remove(txt_file_path)
        logging.info(f"Deleted original .txt file: {txt_file_path}")

    except Exception as e:
        logging.info(f"Error converting {txt_file_path} to CSV: {e}")


def fetch_yyyymm_from_bigquery():
    """Fetch distinct YYYYMM values from BigQuery."""
    query = f"""
        SELECT DISTINCT YYYYMM
        FROM `{project_id}.{dataset_id_q}.{table_id_q}`
        ORDER BY YYYYMM ASC
    """
    try:
        query_job = client.query(query)
        extracted_values = [row["YYYYMM"] for row in query_job]
        if not extracted_values:
            extracted_values = ["000000"]  # Default value if no results
        logging.info("Extracted values from BigQuery: %s", extracted_values)
    except Exception as e:
        logging.error(f"Error querying BigQuery: {e}")
        logging.info("Using default value due to query error.")
        extracted_values = ["000000"]
    return extracted_values


def process_files(files, extracted_values):
    """Process zip files in S3 bucket and compare with extracted BigQuery values."""
    new_files_found = False

    for file in files:
        if file.endswith(".zip"):
            file_date_part = file[
                -10:-4
            ]  # Extracting date part from the file name
            if file_date_part not in extracted_values:
                logging.info(f"Processing new zip file: {file}")
                process_zip_file(
                    file,
                    bucket_name,
                    aws_access_key_id,
                    aws_secret_access_key,
                    region_name,
                    client,
                    dataset_id,
                )
                new_files_found = True  # New files found and processed

    if not new_files_found:
        logging.info("No new files to process.")
        return False
    return True


def fetch_s3_files():
    """Fetch the list of files from the S3 bucket."""
    try:
        files = list_files_in_s3_bucket(
            bucket_name,
            prefix,
            aws_access_key_id,
            aws_secret_access_key,
            region_name,
        )
        logging.info("Files in S3 bucket: %s", files)
        return files
    except Exception as e:
        logging.error(f"Error fetching files from S3: {e}")
        return []
