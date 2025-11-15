import boto3
import json
import logging
from botocore.exceptions import ClientError


def update_secret_field(secret_name, field_key, field_value):
    """
    Updates a specific field in a JSON secret stored in AWS Secrets Manager.
    :param secret_name: Name of the secret.
    :param field_key: Key of the field to update.
    :param field_value: New value of the field.
    """
    # Initialize a session using Amazon Secrets Manager
    session = boto3.Session()
    client = session.client(
        service_name="secretsmanager", region_name="ap-southeast-2"
    )

    try:
        # Fetch the current secret value
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        if "SecretString" in get_secret_value_response:
            secret_string = get_secret_value_response["SecretString"]
            secret_data = json.loads(secret_string)
        else:
            logging.error("Secret does not contain a string value.")
            raise ValueError("Secret format error.")

        # Update the specific field in the JSON object
        secret_data[field_key] = field_value

        # Put the updated JSON object back into AWS Secrets Manager
        client.put_secret_value(
            SecretId=secret_name, SecretString=json.dumps(secret_data)
        )
        logging.info(f"Secret updated successfully.")

    except ClientError as e:
        logging.info(f"Error updating secret: {e}")
        raise e


# Usage example
secret_name = "data_rr"
field_key = "access"
field_value = "new_value"  # New value for the "access" field
update_secret_field(secret_name, field_key, field_value)
