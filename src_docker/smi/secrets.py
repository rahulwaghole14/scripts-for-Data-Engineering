import json

from tomlkit import table
from common.aws.aws_secret import get_secret
from common.aws.aws_s3 import list_files_in_s3_bucket
from common.bigquery.bigquery import create_bigquery_client

smi = get_secret("datateam_smi")
smi_json = json.loads(smi)

# Fixing typo in the secret key name
bucket_name = smi_json["bucket_name"]
prefix = smi_json["prefix"]
aws_access_key_id = smi_json["aws_access_key"]
aws_secret_access_key = smi_json["aws_secret_key"]
region_name = smi_json["region_name"]
google_creds = get_secret("datateam_google_cred_prod_base64")
google_creds = json.loads(google_creds)
google_creds = google_creds.get("GOOGLE_CLOUD_CRED_BASE64")

client = create_bigquery_client(google_creds)
dataset_id = smi_json["dataset_id"]
project_id = smi_json["project_id"]
dataset_id_q = smi_json["dataset_id_q"]
table_id_q = smi_json["table_id_q"]
