import logging
from .functions import (
    process_files,
    fetch_yyyymm_from_bigquery,
    process_files,
    fetch_s3_files,
)
from common.bigquery.bigquery import create_bigquery_client
from common.aws.aws_s3 import list_files_in_s3_bucket
from common.aws.aws_secret import get_secret
from .secrets import (
    bucket_name,
    prefix,
    aws_access_key_id,
    aws_secret_access_key,
    region_name,
    dataset_id,
    project_id,
    client,
    dataset_id_q,
    table_id_q,
)


# Setting up logging
logging.basicConfig(level=logging.INFO)


def main():
    try:
        logging.info(
            "Fetching credentials and configuration from secrets manager"
        )

        # Fetch files from S3
        files = fetch_s3_files()
        if not files:
            logging.info("No files found in the S3 bucket.")
            exit(0)

        # Fetch YYYYMM values from BigQuery
        extracted_values = fetch_yyyymm_from_bigquery()
        print(extracted_values)

        # Process the files
        if not process_files(files, extracted_values):
            logging.info("No new files found to process.")
            exit(0)
    except Exception as e:
        logging.error(f"Error in main function: {e}")
        exit(1)


if __name__ == "__main__":
    main()
