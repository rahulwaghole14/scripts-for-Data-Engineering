import yaml
import json
from common.aws.aws_secret import get_secret

import os

# Get the current working directory
current_directory = os.getcwd()

# Print the current working directory
print(current_directory)


def create_dbt_profile():

    # Define the path for the new profiles.yml file
    new_file_path = "/dbtcdwarehouse/scripts/profile.yml"  # for local testing

    # Fetch the secret from AWS Secrets Manager
    google_sec = get_secret("datateam_google_prod")
    data = json.loads(google_sec)

    # Define the values you want to add to the new file
    new_profile = {
        "cdw": {
            "target": "prod",
            "outputs": {
                "prod": {
                    "type": "bigquery",
                    "location": "australia-southeast1",
                    "method": "service-account-json",
                    "project": data["project_id"],  # Using fetched project ID
                    "dataset": "prod",
                    "threads": 4,
                    "keyfile_json": {
                        "type": "service_account",
                        "project_id": data["project_id"],
                        "private_key_id": data["private_key_id"],
                        "private_key": data["private_key"],
                        "client_email": data["client_email"],
                        "client_id": data["client_id"],
                        "auth_uri": data["auth_uri"],
                        "token_uri": data["token_uri"],
                        "auth_provider_x509_cert_url": data[
                            "auth_provider_x509_cert_url"
                        ],
                        "client_x509_cert_url": data["client_x509_cert_url"],
                    },
                }
            },
        }
    }

    # Save the new data to the new file
    with open(new_file_path, "w") as file:
        yaml.safe_dump(new_profile, file, default_flow_style=False)

    print(f"profile_new.yml has been created.")
    current_directory = os.getcwd()
    print(current_directory)


# To execute the function:
