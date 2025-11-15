import sys
import gzip
import os
import json
import time
from datetime import datetime, timedelta
from typing import List
import boto3
from google.cloud import bigquery
from dotenv import load_dotenv
from pydantic import ValidationError

# Add the path for custom modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from megaphone.validator import Impression, Metrics
from common.logging.logger2 import get_logger

# Initialize custom logger
logger = get_logger("megaphone_data_to_bigquery")

# Load environment variables from .env file
load_dotenv()

def validate_impression_data(json_data: List[dict]):
    valid_data = []
    for record in json_data:
        try:
            validated_record = Impression(**record)
            valid_data.append(validated_record.model_dump())
        except ValidationError as e:
            logger.error(f"Validation error for record: {record} - {e}")
    return valid_data

def validate_metrics_data(json_data: List[dict]):
    valid_data = []
    for record in json_data:
        try:
            validated_record = Metrics(**record)
            valid_data.append(validated_record.model_dump())
        except ValidationError as e:
            logger.error(f"Validation error for record: {record} - {e}")
    return valid_data

# Get Google Cloud credentials from .env file
google_creds_json = os.getenv("GOOGLE_CLOUD_CRED")

if google_creds_json is None:
    logger.error("Google Cloud credentials not found in .env file")
    raise ValueError("Google Cloud credentials not found in .env file")

logger.info('Google Cloud credentials loaded successfully')

# Set the Google Cloud credentials directly from the .env file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_creds_json

# Get AWS credentials and bucket name from .env file
megaphone_aws_access_key_id = os.getenv('MEGAPHONE_AWS_ACCESS_KEY_ID')
megaphone_aws_secret_access_key = os.getenv('MEGAPHONE_AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('MEGAPHONE_BUCKET_NAME')

if megaphone_aws_access_key_id is None or megaphone_aws_secret_access_key is None or bucket_name is None:
    logger.error("megaphone_aws_access_key_id, megaphone_aws_secret_access_key, or MEGAPHONE_BUCKET_NAME not set in .env file")
    raise ValueError("megaphone_aws_access_key_id, megaphone_aws_secret_access_key, or MEGAPHONE_BUCKET_NAME not set in .env file")

logger.info("AWS credentials and bucket name loaded successfully")

def download_and_unzip_from_s3(bucket_name, file_key):
    start_time = time.time()
    s3 = boto3.client(
        's3',
        aws_access_key_id=megaphone_aws_access_key_id,
        aws_secret_access_key=megaphone_aws_secret_access_key
    )
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    with gzip.GzipFile(fileobj=obj['Body']) as gzipfile:
        content = gzipfile.read().decode('utf-8')
    elapsed_time = time.time() - start_time
    logger.info(f"Downloaded and unzipped {file_key} from S3 bucket {bucket_name} in {elapsed_time:.2f} seconds")
    return content

def load_json_to_bigquery(json_data, table_id, autodetect_schema=True):
    client = bigquery.Client.from_service_account_info(json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")))

    # Add load_dts field to each record
    load_timestamp = datetime.now().isoformat()
    json_records = []
    for line in json_data:
        try:
            record = json.loads(line)
            record['load_dts'] = load_timestamp
            json_records.append(record)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing line: {line}")
            raise ValueError("Error parsing JSON data") from e

    # Validate records using Pydantic models
    if 'impression' in table_id:
        json_records = validate_impression_data(json_records)
    elif 'metrics' in table_id:
        json_records = validate_metrics_data(json_records)

    # Ensure all datetime fields are converted to strings
    for record in json_records:
        for key, value in record.items():
            if isinstance(value, datetime):
                record[key] = value.isoformat()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=autodetect_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at"
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    load_job = client.load_table_from_json(
        json_records,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete
    logger.info(f"Loaded {load_job.output_rows} rows into {table_id}")

def main():
    yesterday = datetime.now() - timedelta(1)
    date_suffix = yesterday.strftime('%Y%m%d')

    # Define the file keys for metrics and impression files
    metrics_file_key = yesterday.strftime('metrics-day-%Y-%m-%d.json.gz')
    impression_file_key = yesterday.strftime('impression-day-%Y-%m-%d.json.gz')

    # Define BigQuery table IDs with date suffix
    metrics_table_id = f'hexa-data-report-etl-prod.cdw_stage.metrics_{date_suffix}'
    impression_table_id = f'hexa-data-report-etl-prod.cdw_stage.impression_{date_suffix}'

    # Download, unzip, and load metrics file
    logger.info("Processing metrics file...")
    metrics_json_data = download_and_unzip_from_s3(bucket_name, metrics_file_key)
    metrics_json_lines = metrics_json_data.splitlines()
    load_json_to_bigquery(metrics_json_lines, metrics_table_id)

    # Download, unzip, and load impression file
    logger.info("Processing impression file...")
    impression_json_data = download_and_unzip_from_s3(bucket_name, impression_file_key)
    impression_json_lines = impression_json_data.splitlines()

    # Debugging: Log the first two lines of impression data
    logger.debug("First two lines of impression JSON data:")
    for line in impression_json_lines[:2]:
        logger.debug(line)

    load_json_to_bigquery(impression_json_lines, impression_table_id)

if __name__ == "__main__":
    main()
