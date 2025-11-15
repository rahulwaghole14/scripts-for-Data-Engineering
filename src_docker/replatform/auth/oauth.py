import requests
import json
import os
import logging
import boto3
from botocore.exceptions import ClientError


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


def generate_oauth():

    data_replatform = get_secret("datateam_replatform")
    data_replatform_json = json.loads(data_replatform)

    CLIENT_ID = data_replatform_json["client_id"]
    CLIENT_SECRET = data_replatform_json["client_secret"]

    URL = "https://ims-na1.adobelogin.com/ims/token/v3"

    HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

    DATA = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "openid,AdobeID,additional_info.projectedProductContext",
    }
    print("data", DATA)

    x = requests.post(url=URL, headers=HEADERS, data=DATA)
    print("x", x)

    z = json.loads(x.text)
    return z
