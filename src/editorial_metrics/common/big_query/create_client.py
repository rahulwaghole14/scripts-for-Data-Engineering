import os
from google.cloud import bigquery
from google.oauth2 import service_account
from common.paths import SECRET_PATH


def create_bq_client():
    key_path = os.path.join(str(SECRET_PATH), 'hexa-data-report-etl-prod-f89b703bc217.json')


    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials)
    return client