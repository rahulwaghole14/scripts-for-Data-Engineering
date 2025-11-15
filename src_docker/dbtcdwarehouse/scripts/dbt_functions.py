""" run dbt """

# from pdb import run
import os
import sys  # Import the sys module
import subprocess
import re
import logging
import json
import yaml
from common.aws.aws_secret import get_secret
from .logger import logger

logger("dbt-sent")

# Initialize logging
logging.basicConfig(
    filename="dbt_run.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def clean_ansi_codes(string):
    """clean output"""
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_escape.sub("", string)


def parse_failure_messages(data):
    """Parse failure messages from dbt output."""
    fail_msgs = [line for line in data if "ERROR" in line]
    return fail_msgs


def run_dbt_freshness(profiles_dir, project_dir):
    """Run dbt freshness."""

    dbt_command = [
        "dbt",
        "source",
        "freshness",
        "--profiles-dir",
        profiles_dir,
        "--project-dir",
        project_dir,
    ]

    stdout_data = []

    try:

        with subprocess.Popen(
            dbt_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as process:

            # Collect stdout in real-time
            for line in iter(process.stdout.readline, b""):
                clean_line = clean_ansi_codes(line.decode("utf-8").strip())
                logging.info("STDOUT: %s", clean_line)
                stdout_data.append({"line": clean_line})

    except Exception as error:  # pylint: disable=broad-except
        logging.exception("An unexpected error occurred. %s", error)
        sys.exit(1)  # Exit with status code 1 to signal that the script failed

    return stdout_data


def run_dbt_build(cmd, tag, profiles_dir, project_dir):
    """
    This function executes a dbt build command targeting models with the "+tag:sentiment" tag.

    It retrieves a secret named "datateam_piano_cred", extracts a value named "UUID_NAMESPACE",
    sets an environment variable with that value, and then proceeds with the dbt build process.
    It captures standard output (stdout) and standard error (stderr) during execution
    and logs them appropriately. In case of errors, it raises a RuntimeError.
    """
    try:

        # Get secret (assuming get_secret function exists)
        data = get_secret("datateam_piano_cred")
        data_json = json.loads(data)
        uuid_namespace = data_json["UUID_NAMESPACE"]

        # Set environment variable
        os.environ["UUID_NAMESPACE"] = uuid_namespace

        # profiles_dir = "/usr/src/app/dbtcdwarehouse"
        # project_dir = "/usr/src/app/dbtcdwarehouse"

        # Define dbt command
        dbt_command = [
            "dbt",
            cmd,
            "--select",
            tag,
            "--profiles-dir",
            profiles_dir,
            "--project-dir",
            project_dir,
        ]

        # Execute the command

        stdout_data = []

        with subprocess.Popen(
            dbt_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as process:

            # Collect stdout in real-time
            for line in iter(process.stdout.readline, b""):
                clean_line = clean_ansi_codes(line.decode("utf-8").strip())
                logging.info("STDOUT: %s", clean_line)
                stdout_data.append({"line": clean_line})

    except Exception as error:  # pylint: disable=broad-except
        logging.exception("An unexpected error occurred. %s", error)
        sys.exit(1)  # Exit with status code 1 to signal that the script failed


def update_dbt_profile(google_sec):
    """
    This function updates the dbt profiles.yml file with the provided Google Cloud credentials.
    """
    try:
        # Define the path for the profiles.yml file
        file_path = "dbtcdwarehouse/profiles.yml"

        # Check if the file exists
        if not os.path.exists(file_path):
            # Create an empty dictionary if the file doesn't exist
            data = {}
        else:
            logging.info("%s already exists.", file_path)
            # Load the existing data if the file exists
            with open(file_path, "r", encoding="utf-8") as file:
                data = yaml.safe_load(file)

        # Fetch the secret from AWS Secrets Manager
        data = json.loads(google_sec)
        tar = data["target"]

        # Define the values you want to add to the file
        updates = {
            "cdw": {
                "target": tar,
                "outputs": {
                    tar: {
                        "type": "bigquery",
                        "location": data["location"],
                        "method": "service-account-json",
                        "project": data[
                            "project_id"
                        ],  # Using fetched project ID
                        "dataset": data["dataset"],
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
                            "client_x509_cert_url": data[
                                "client_x509_cert_url"
                            ],
                        },
                    }
                },
            }
        }

        # Update the existing data with new values
        data.update(updates)

        # Save the updated data back to the file
        with open(file_path, "w", encoding="utf-8") as file:
            yaml.safe_dump(data, file, default_flow_style=False)

        logging.info("profiles.yml has been updated.")

    except Exception as e:  # pylint: disable=broad-except
        logging.info("An error occurred: %s", e)


def print_profiles_content():
    """
    This function reads the content of the profiles.yml file and pretty-logging.infoes it.
    """

    try:
        with open("profiles.yml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            logging.info(
                yaml.dump(data, default_flow_style=False)
            )  # Pretty-logging.infoed output

    except FileNotFoundError:
        logging.info("profiles.yml file not found.")

    except Exception as e:  # pylint: disable=broad-except
        logging.info("An error occurred: %s", e)


# Call the function to logging.info the content


def check_environment():
    """
    This function checks the current working directory
    and prints messages based on the location and value.

    Args:
        value: A string value to be compared (assumed defined elsewhere in your script).
    """
    current_dir = os.getcwd()
    logging.info("Current directory: %s", current_dir)

    if current_dir == "/usr/src/app":
        value = "Docker"
    else:
        value = "Local"
    return value
