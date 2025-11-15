import boto3
import sys
import gzip
import os
import json
import time
from google.cloud import bigquery
from datetime import datetime
from typing import List

# Add the path for custom modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.aws.aws_secret import get_secret

# Fetch Google Cloud credentials from AWS Secrets Manager
google_creds_json = get_secret("datateam_google_prod")

if google_creds_json is None:
    raise ValueError("Google Cloud credentials not found in AWS Secrets Manager")

print('Google Cloud credentials loaded successfully')

# Set the Google Cloud credentials directly from the secret
os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = google_creds_json

# Get AWS credentials from AWS Secrets Manager
secret_value = get_secret("datateam_megaphone")
secret_json = json.loads(secret_value)
aws_access_key_id = secret_json['MEGAPHONE_AWS_ACCESS_KEY_ID']
aws_secret_access_key = secret_json['MEGAPHONE_AWS_SECRET_ACCESS_KEY']
bucket_name = secret_json['MEGAPHONE_BUCKET_NAME']

if aws_access_key_id is None or aws_secret_access_key is None or bucket_name is None:
    raise ValueError("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or MEGAPHONE_BUCKET_NAME not set")

print("AWS credentials and bucket name loaded successfully")

def list_files_in_s3(bucket_name, prefix=''):
    print("Listing files in S3 bucket...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    print(f"Found {len(files)} files in S3 bucket.")
    return files

def filter_files(files, start_date_str, end_date_str):
    print(f"Filtering files from {start_date_str} to before {end_date_str}...")
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    filtered = []

    for f in files:
        if 'impression-day-' in f or 'metrics-day-' in f:
            try:
                file_date_str = f.split('-day-')[-1].split('.')[0]
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                if start_date <= file_date < end_date:
                    filtered.append(f)
            except Exception as e:
                print(f"Error processing file {f}: {e}")

    print(f"Filtered down to {len(filtered)} files.")
    return filtered

def download_and_unzip_from_s3(bucket_name, file_key):
    print(f"Downloading and unzipping file {file_key}...")
    start_time = time.time()
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    with gzip.GzipFile(fileobj=obj['Body']) as gzipfile:
        content = gzipfile.read().decode('utf-8')
    elapsed_time = time.time() - start_time
    print(f"Downloaded and unzipped {file_key} from S3 bucket {bucket_name} in {elapsed_time:.2f} seconds")
    return content

def load_json_to_bigquery(json_data, table_id, autodetect_schema=True):
    client = bigquery.Client.from_service_account_info(json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")))

    # Add load_dts field to each record
    load_timestamp = datetime.now().isoformat()
    json_records = []
    for line in json_data:
        try:
            record = json.loads(line)
            record['load_dts'] = load_timestamp
            json_records.append(record)
        except json.JSONDecodeError as e:
            print(f"Error parsing line: {line}")
            raise ValueError("Error parsing JSON data") from e

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=autodetect_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at"  # Assuming your JSON has a 'created_at' field
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    load_job = client.load_table_from_json(
        json_records,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete
    print(f"Loaded {load_job.output_rows} rows into {table_id}")

def main():
    start_date = '2024-06-09'
    end_date = '2024-06-10'
    dataset_id = 'hexa-data-report-etl-prod.cdw_stage'
    metrics_table_base = f'{dataset_id}.metrics_'
    impression_table_base = f'{dataset_id}.impression_'

    # List all files in the S3 bucket
    all_files = list_files_in_s3(bucket_name)

    # Filter files to include only those between the start date and end date
    filtered_files = filter_files(all_files, start_date, end_date)

    # Process each file
    for file_key in filtered_files:
        # Determine the table base name and suffix based on the file name
        if 'metrics-day-' in file_key:
            table_base = metrics_table_base
        elif 'impression-day-' in file_key:
            table_base = impression_table_base
        else:
            continue
        
        # Extract date suffix from file name to determine the correct table
        file_date_str = file_key.split('-day-')[-1].split('.')[0]
        table_id = f'{table_base}{file_date_str.replace("-", "")}'

        # Download, unzip, and load the file
        print(f"Processing file {file_key} into table {table_id}...")
        try:
            json_data = download_and_unzip_from_s3(bucket_name, file_key)
            json_lines = json_data.splitlines()

            # Load data into BigQuery
            load_json_to_bigquery(json_lines, table_id)
        except Exception as e:
            print(f"Failed to process file {file_key}: {e}")
            # Log the error and continue with the next file

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Script encountered an error: {e}")
    print("Script finished.")
