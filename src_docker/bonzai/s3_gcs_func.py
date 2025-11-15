import json
import boto3
import pandas as pd
from google.cloud import storage
from datetime import datetime
from io import BytesIO
from common.aws.aws_secret import get_secret
from google.oauth2 import service_account

# Configuration
aws_creds = get_secret("dateam_bonzai_s3")
google_creds = get_secret("datateam_google_prod")
aws_creds_json = json.loads(aws_creds)
google_creds_json = json.loads(google_creds)

AWS_ACCESS_KEY = aws_creds_json["access_key"]
AWS_SECRET_KEY = aws_creds_json["secret_key"]
SOURCE_BUCKET = "drive-bonzai-reporting-storage"
GCS_BUCKET = "bonzai_staging"
MANIFEST_FILE_NAME = "manifest.csv"

# Initialize clients
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

credentials = service_account.Credentials.from_service_account_info(google_creds_json)
gcs_client = storage.Client(credentials=credentials)


def list_all_s3_folders(bucket):
    """List all folders (prefixes) in the S3 bucket."""
    folders = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            folders.add(prefix["Prefix"])
    return sorted(folders)


def list_s3_files(bucket, prefix):
    """List all Parquet files in the S3 bucket with the given prefix."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])
    return files


def stream_parquet_to_gcs(s3_bucket, s3_key, gcs_bucket, gcs_file_name):
    """Stream a Parquet file from S3 to GCS without using local storage."""
    s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    s3_stream = s3_object["Body"]

    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)
    with blob.open("wb") as gcs_stream:
        for chunk in s3_stream.iter_chunks(chunk_size=1024 * 1024):  # 1 MB chunks
            gcs_stream.write(chunk)

    print(f"Streamed {s3_key} to GCS as {gcs_file_name}")
    return f"gs://{gcs_bucket}/{gcs_file_name}"


def load_manifest(gcs_bucket):
    """Load the manifest file from GCS, if it exists."""
    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(MANIFEST_FILE_NAME)

    if blob.exists():
        csv_buffer = BytesIO()
        blob.download_to_file(csv_buffer)
        csv_buffer.seek(0)
        manifest_df = pd.read_csv(csv_buffer)
        print("Loaded existing manifest file from GCS.")
        return manifest_df
    else:
        print("No existing manifest file found. Starting fresh.")
        return pd.DataFrame(columns=["s3_path", "gcs_path", "processed_at"])


def save_manifest(manifest_df, gcs_bucket):
    """Save the manifest file to GCS."""
    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(MANIFEST_FILE_NAME)

    csv_buffer = BytesIO()
    manifest_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    blob.upload_from_file(csv_buffer)
    print("Updated manifest file saved to GCS.")


def extract_date_from_key(file_key):
    """Extract year, month, and day from the S3 file key."""
    try:
        year = file_key.split("year=")[1].split("/")[0]
        month = file_key.split("month=")[1].split("/")[0]
        day = file_key.split("day=")[1].split("/")[0]
        date_str = f"{year}{month.zfill(2)}{day.zfill(2)}"
        return date_str
    except IndexError:
        print(f"Skipping file with unexpected format: {file_key}")
        return None


def replicate_all_to_gcs():
    """Main function to replicate files from all folders in S3 to GCS."""
    folders = list_all_s3_folders(SOURCE_BUCKET)
    if not folders:
        print("No folders found to process.")
        return

    manifest_df = load_manifest(GCS_BUCKET)
    processed_files = set(manifest_df["s3_path"])

    for folder in folders:
        print(f"Processing folder: {folder}")

        files_to_process = list_s3_files(SOURCE_BUCKET, folder)
        if not files_to_process:
            print(f"No files found in folder: {folder}")
            continue

        records = []
        for file_key in files_to_process:
            # Skip files that have already been processed
            if file_key in processed_files:
                print(f"File already processed: {file_key}. Skipping.")
                continue

            # Extract date from the file key
            date_str = extract_date_from_key(file_key)
            if not date_str:
                continue

            # Generate the new file name based on the extracted date
            new_file_name = f"data_{date_str}.parquet"

            # Stream Parquet file from S3 to GCS with the new name
            gcs_uri = stream_parquet_to_gcs(
                SOURCE_BUCKET, file_key, GCS_BUCKET, new_file_name
            )

            # Record the processed file with the original S3 key
            records.append(
                {
                    "s3_path": file_key,
                    "gcs_path": gcs_uri,
                    "processed_at": datetime.now().isoformat(),
                }
            )

        # Append new records to the manifest DataFrame and save it
        if records:
            new_manifest_df = pd.DataFrame(records)
            manifest_df = pd.concat([manifest_df, new_manifest_df], ignore_index=True)
            save_manifest(manifest_df, GCS_BUCKET)


# if __name__ == "__main__":
#     replicate_all_to_gcs()
