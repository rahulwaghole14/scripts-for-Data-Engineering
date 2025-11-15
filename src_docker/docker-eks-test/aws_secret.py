"""
   A function to retreve a secrets from aws secrets manager.
"""
import logging
import boto3
from botocore.exceptions import ClientError


def get_secret():
    """
    Fetches the secret from AWS Secrets Manager using specific credentials.
    :return: The secret string.
    """
    # Initialize session with AWS Secrets Manager using credentials
    session = boto3.Session()
    secret_name = "google_cred"
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
