"""
This module is designed to load lookup table to big query
"""

from google.cloud import bigquery


def load_table(
    bucket_name: str, directory_path: str, table_name: str, dataset_id: str
):
    """
    Loads a table to BigQuery from a CSV file stored in a Google Cloud Storage bucket.

    This function takes the name of the bucket, the path to the directory in the bucket,
    the name of the table, and the ID of the dataset in BigQuery, then loads the table with
    the specified schema. The table is overwritten if it already exists.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket where the CSV file is stored.
        directory_path (str): The path to the directory in the bucket where the CSV file is located.
        table_name (str): The name of the table to be loaded.
        dataset_id (str): The ID of the dataset in BigQuery where the table is to be loaded.

    Side Effects:
        Loads the specified table in BigQuery, overwriting if it already exists.
        Prints a message indicating that the table has been loaded.
    """
    bigquery_client = bigquery.Client()

    # Specify the schema explicitly
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assumes that your file has headers, change accordingly
        field_delimiter="\t",
        write_disposition="WRITE_TRUNCATE",
        max_bad_records=100,  # Overwrite the existing table with the new data
    )

    # URI of the blob, adjust the path as needed
    uri = f"gs://{bucket_name}/{directory_path}/{table_name}.tsv"

    # Construct the table_id using the dataset_id and table_name
    table_id = f"{dataset_id}.{table_name}"

    # Load the table
    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.

    # Print the result
    print(f"{table_name} loaded to {table_id}.")
