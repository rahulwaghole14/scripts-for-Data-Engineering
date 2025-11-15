import json
import os
import gcsfs
import logging
import sys
from google.oauth2 import service_account
from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns
from .functions import load_parquet_as_string_to_bigquery, delete_gcs_file
from .s3_gcs_func import replicate_all_to_gcs
from dbtcdwarehouse.scripts.dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)


# Initialize logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("dbt_run.log", mode="a"),
        ],
    )


# Example Usage
def main(project_id, dataset_id, gcs_bucket_name, google_creds_json):  # Configuration
    replicate_all_to_gcs()

    # Define OAuth scopes
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.read_only",
    ]

    # Initialize credentials
    credentials = service_account.Credentials.from_service_account_info(
        google_creds_json, scopes=scopes
    )

    # Initialize GCS filesystem
    fs = gcsfs.GCSFileSystem(token=credentials)

    # List all Parquet files in the GCS bucket
    parquet_files = fs.glob(f"gs://{gcs_bucket_name}/*.parquet")
    logging.info(f"Found Parquet files: {parquet_files}")

    if not parquet_files:
        logging.info(f"No Parquet files found in GCS bucket: {gcs_bucket_name}")
    else:
        # Loop through each file and process it
        for file in parquet_files:
            gcs_file_path = f"gs://{file}"
            logging.info(f"Processing file: {gcs_file_path}")
            table_id = (
                file.split("/")[-1]
                .replace(".parquet", "")
                .replace("-", "_")
                .replace(".", "_")
            )
            logging.info(f"Table ID: {table_id}")

            # Load the Parquet file into BigQuery with all columns as STRING
            load_parquet_as_string_to_bigquery(
                project_id=project_id,
                credentials=credentials,
                dataset_id=dataset_id,
                table_id=table_id,
                gcs_file_path=gcs_file_path,
                write_disposition="WRITE_APPEND",
            )

            delete_gcs_file(credentials, gcs_file_path)


if __name__ == "__main__":
    setup_logging()
    google_creds = get_secret("datateam_google_prod")
    google_creds_json = json.loads(google_creds)
    data_bonzai = get_secret("dateam_bonzai_s3")
    data_bonzai_json = json.loads(data_bonzai)
    project_id = data_bonzai_json["project_id"]
    dataset_id = data_bonzai_json["dataset_id"]
    gcs_bucket_name = data_bonzai_json["gcs_bucket_name"]
    google_sec = get_secret("datateam_dbt_creds")
    data_json = json.loads(google_sec)
    UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
    os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
    main(project_id, dataset_id, gcs_bucket_name, google_creds_json)
    value = check_environment()
    if value == "Docker":
        logging.info("Environment is Docker.")
        update_dbt_profile(google_sec)
        profiles_dir = "/usr/src/app/dbtcdwarehouse"
        project_dir = "/usr/src/app/dbtcdwarehouse"
    cmd = "build"
    tag = "+tag:bonzai"
    logging.info(f"Running dbt {cmd} with tag {tag}")
    run_dbt_build(cmd, tag, profiles_dir, project_dir)
    logging.info("dbt build completed successfully.")
    publish_message_to_sns(
        "datateam_eks_notifications",
        "bonzai and dbt bonzai build completed successfully.",
    )
    sys.exit(0)  # Exit with status code 0 to signal successful completion
