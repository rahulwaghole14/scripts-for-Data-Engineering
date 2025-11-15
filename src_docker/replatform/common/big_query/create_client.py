import os
import logging
import boto3
import json
from botocore.exceptions import ClientError
from google.cloud import bigquery
from google.oauth2 import service_account


def get_secret(secret_name):
    """
    Fetches the secret from AWS Secrets Manager using specific credentials.
    :param secret_name: Name of the secret to retrieve.
    :return: The secret string.
    """
    # Initialize session with AWS Secrets Manager using credentials
    session = boto3.Session()
    client = session.client(
        service_name="secretsmanager", region_name="ap-southeast-2"
    )

    try:
        # Fetch the secret value
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        if "SecretString" in get_secret_value_response:
            return get_secret_value_response["SecretString"]
        logging.error("Secret does not contain a string value.")
        raise ValueError("Secret format error.")
    except ClientError as ex:
        logging.error("Error retrieving secret: %s", ex)
        raise ex


def create_bq_client():
    secret_str = get_secret("datateam_google_prod")
    google_creds = json.loads(secret_str)
    # key_path = os.path.join(str(SECRET_PATH), 'hexa-data-report-etl-prod-f89b703bc217.json')
    credentials = service_account.Credentials.from_service_account_info(
        google_creds, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    client = bigquery.Client(
        credentials=credentials, project=google_creds["project_id"]
    )
    return client
