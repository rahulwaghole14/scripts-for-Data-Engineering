"""
   A function to retreve a secrets from aws secrets manager.
"""
import logging
import boto3
from botocore.exceptions import ClientError


def get_secret(
    access_key: str,
    secret_key: str,
    session_token: str,
    secret_name: str,
    region_name: str,
) -> str:
    """
    Fetches the secret from AWS Secrets Manager using specific credentials.

    :param access_key: AWS access key ID.
    :param secret_key: AWS secret access key.
    :param session_token: AWS session token for temporary credentials.
    :param secret_name: Name of the secret in AWS Secrets Manager.
    :param region_name: AWS region where the secret is stored.
    :return: The secret string.
    """
    # Initialize session with AWS Secrets Manager using credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region_name,
    )
    client = session.client("secretsmanager")

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
