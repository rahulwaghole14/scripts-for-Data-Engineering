from google.cloud import bigquery
from google.cloud import storage
from google.oauth2.service_account import Credentials
from common.aws.aws_secret import get_secret
import boto3
import json
from io import BytesIO

# Retrieve Google Cloud and AWS credentials from Secrets Manager
try:
    data_creds = get_secret("datateam_google_prod")  # Replace with your secret name
    data_creds_json = json.loads(data_creds)
except json.JSONDecodeError:
    raise ValueError("Invalid JSON format for Google Cloud credentials.")

try:
    infosum_creds = get_secret("datateam_infosum")  # Replace with your secret name
    infosum_creds_json = json.loads(infosum_creds)
except json.JSONDecodeError:
    raise ValueError("Invalid JSON format for AWS credentials.")

# Create Google Cloud credentials from the JSON
credentials = Credentials.from_service_account_info(data_creds_json)

# Initialize BigQuery and GCS Clients with the credentials
bq_client = bigquery.Client(
    credentials=credentials, project=data_creds_json["project_id"]
)
gcs_client = storage.Client(
    credentials=credentials, project=data_creds_json["project_id"]
)

# AWS Credentials
aws_access_key = infosum_creds_json.get("aws_access_key")
aws_secret_key = infosum_creds_json.get("aws_secret_key")
aws_region = infosum_creds_json.get("aws_region")

# Initialize S3 client with credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region,
)


# Function to move data from BigQuery to GCS
def move_data_to_gcs(project_id, dataset_id, table_id, bucket_name, destination_path):
    """
    Export data from a BigQuery table to a Google Cloud Storage bucket.
    """
    try:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        destination_uri = f"gs://{bucket_name}/{destination_path}"

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = "CSV"  # You can change this to another
        # format

        extract_job = bq_client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
        )  # Make an API request
        extract_job.result()  # Wait for the job to complete

        print(f"Exported data from {table_ref} to {destination_uri}")
    except Exception as e:
        print(f"Error exporting data to GCS: {e}")


# Function to transfer files from GCS to S3
def stream_gcs_to_s3(gcs_bucket_name, s3_bucket_name):
    try:
        gcs_bucket = gcs_client.bucket(gcs_bucket_name)
        blobs = gcs_bucket.list_blobs()  # List all files in the GCS bucket

        for blob in blobs:
            print(f"Streaming {blob.name} from GCS to S3...")

            # Read the file data from GCS as a stream
            gcs_stream = BytesIO()
            blob.download_to_file(gcs_stream)
            gcs_stream.seek(0)  # Reset stream position for reading

            # Stream data directly to S3ƒd
            s3_key = blob.name  # Use the same name for the file in S3
            s3_client.upload_fileobj(gcs_stream, s3_bucket_name, s3_key)

            print(
                f"Successfully streamed {blob.name} to S3: s3://{s3_bucket_name}/{s3_key}"
            )

    except Exception as e:
        print(f"Error during streaming: {e}")
