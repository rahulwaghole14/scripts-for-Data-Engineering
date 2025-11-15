""" create bigquery client """ ""

from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from admanagerworkflow.load_config import (
    load_config,
)  # Import the load_config function


def create_bigquery_client():
    config = load_config()  # Load configuration
    google_cred = config["google_cred"]

    try:
        # Assuming google_cred is a dictionary with the credentials
        credentials = service_account.Credentials.from_service_account_info(
            google_cred,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        logging.info("Created BigQuery client")
    except Exception as error:
        logging.error("Failed to create BigQuery client: %s", error)
        client = None  # Assign a default value to client in case of error

    return client
