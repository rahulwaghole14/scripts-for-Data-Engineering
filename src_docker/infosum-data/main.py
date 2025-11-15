import json

from .function import move_data_to_gcs, stream_gcs_to_s3
from common.aws.aws_secret import get_secret

# Example usage
if __name__ == "__main__":
    # Fetch Infosum credentials from AWS Secrets Manager
    data_infosum_creds = get_secret("datateam_infosum")
    data_infosum_creds_json = json.loads(data_infosum_creds)

    # BigQuery to GCS configurations
    project_id = data_infosum_creds_json["project_id"]
    dataset_id = data_infosum_creds_json["dataset_id"]
    table_id = data_infosum_creds_json["table_id"]
    destination_path = data_infosum_creds_json["destination_path"]
    gcs_bucket_name = data_infosum_creds_json[
        "gcs_bucket_name"
    ]  # Replace with your GCS bucket name

    # S3 configurations
    s3_bucket_name = data_infosum_creds_json[
        "s3_bucket_name"
    ]  # Replace with your S3 bucket name

    # Step 1: Export data from BigQuery to GCS
    print("Exporting data from BigQuery to GCS...")
    move_data_to_gcs(
        project_id, dataset_id, table_id, gcs_bucket_name, destination_path
    )

    # Step 2: Stream data from GCS to S3
    print("Streaming data from GCS to S3...")
    stream_gcs_to_s3(gcs_bucket_name, s3_bucket_name)
