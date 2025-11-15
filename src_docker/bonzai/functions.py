import pyarrow.parquet as pq
import gcsfs
from google.cloud import bigquery
from io import StringIO
from google.cloud import storage


def load_parquet_as_string_to_bigquery(
    project_id: str,
    credentials,
    dataset_id: str,
    table_id: str,
    gcs_file_path: str,
    write_disposition: str = "WRITE_APPEND",
):
    """
    Load Parquet data from GCS into BigQuery with all columns as STRING type.

    Args:
        project_id (str): GCP project ID.
        credentials: Service account credentials object.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        gcs_file_path (str): Full GCS file path (e.g., "gs://bucket/file.parquet").
        write_disposition (str): BigQuery write mode. Options: "WRITE_APPEND", "WRITE_TRUNCATE".

    Returns:
        None
    """
    try:
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=project_id)

        # Initialize GCS filesystem
        fs = gcsfs.GCSFileSystem(token=credentials)

        # Read the Parquet file into a DataFrame using pyarrow
        print(f"Reading Parquet file: {gcs_file_path}")
        with fs.open(gcs_file_path, "rb") as f:
            parquet_file = pq.ParquetFile(f)
            df = parquet_file.read().to_pandas()

        # Convert all columns to STRING type
        df = df.astype(str)

        # Check and print the actual column names
        # print(f"Column names: {df.columns.tolist()}")

        # Convert DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Define the BigQuery table reference
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # BigQuery job configuration for CSV format
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=write_disposition,
            skip_leading_rows=1,
        )

        # Load data to BigQuery from the CSV buffer
        print(f"Starting load job for {gcs_file_path} to {table_ref}...")
        load_job = client.load_table_from_file(
            csv_buffer, table_ref, job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()

        print(
            f"Data from {gcs_file_path} successfully loaded into {table_ref} with all columns as STRING."
        )
    except Exception as e:
        print(f"An error occurred while loading {gcs_file_path}: {str(e)}")


def delete_gcs_file(credentials, gcs_file_path: str):
    """
    Delete a file from Google Cloud Storage (GCS).

    Args:
        credentials: Service account credentials object.
        gcs_file_path (str): Full GCS file path (e.g., "gs://bucket/file.parquet").

    Returns:
        None
    """
    try:
        # Initialize GCS client using google-auth compatible credentials
        storage_client = storage.Client(credentials=credentials)

        # Extract the bucket name and blob (file) name from the GCS URI
        if gcs_file_path.startswith("gs://"):
            gcs_file_path = gcs_file_path[5:]
        bucket_name, blob_name = gcs_file_path.split("/", 1)

        # Get the bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Delete the file
        print(f"Deleting file: {gcs_file_path}")
        blob.delete()
        print(f"File {gcs_file_path} successfully deleted from GCS.")
    except Exception as e:
        print(f"An error occurred while deleting {gcs_file_path}: {str(e)}")
