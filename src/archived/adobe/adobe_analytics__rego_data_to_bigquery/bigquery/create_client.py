""" create bigquery client """ ""

import json
from google.cloud import bigquery
from google.oauth2 import service_account
import logging


def create_bigquery_client(env_credentials):
    """create bigquery client"""
    google_cred = env_credentials

    try:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(google_cred),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

        logging.info("created bigquery client")
    except Exception as error:
        logging.info(error)

    return client
