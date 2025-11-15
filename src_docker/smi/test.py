import csv
import os
from common.aws.aws_s3 import list_files_in_s3_bucket
from .secrets import (
    bucket_name,
    prefix,
    aws_access_key_id,
    aws_secret_access_key,
    region_name,
    dataset_id,
    project_id,
    client,
)

# List files in the S3 bucket
# files = list_files_in_s3_bucket(
#             bucket_name,
#             prefix,
#             aws_access_key_id,
#             aws_secret_access_key,
#             region_name,
#         )

# # Print list of files and extract date parts from filenames
# print(files)
# for file in files:
#     if file.endswith(".zip"):
#         file_date_part = file[-10:-4]  # Extract the date part (e.g., 202401)
#         print(file_date_part)

csv_file_path = "data/20240101_data.csv"
# BigQuery project and dataset details
table_id = os.path.splitext(os.path.basename(csv_file_path))[0]
table_id = "_".join(table_id.split("_")[:-1])
table_id = table_id.lower()
print(table_id)
