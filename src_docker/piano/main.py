"""
Main script to run Piano tasks
"""

import sys
import logging
import os
import json
from datetime import datetime

from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns

from dbtcdwarehouse.scripts.dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)

from .conversions.conversions import conversions
from .vxlog.main import main as vxlog
from .converted.converted import converted
from .subbq.subbq import subbq


def main():
    """Main function to run Piano tasks"""
    try:
        # Execute Piano conversion functions
        logging.info("== START PIANO MAIN SCRIPT ==")
        start_time = datetime.now()
        converted()
        subbq()
        vxlog()
        conversions()

        logging.info("Piano tasks completed successfully")
        publish_message_to_sns(
            "datateam_eks_notifications", "Piano tasks completed"
        )
    except Exception as error:  # pylint: disable=broad-except
        logging.exception(
            "An unexpected error occurred during Piano tasks: %s", error
        )
        publish_message_to_sns(
            "datateam_eks_notifications", f"Error running Piano tasks: {error}"
        )
        sys.exit(1)  # Exit with status code 1 to signal an error

    try:
        logging.info("Running dbt Piano Mart")

        # Get secrets for DBT from AWS Secrets Manager
        # google_sec = get_secret("datateam_dbt_cred_dev")
        google_sec = get_secret("datateam_dbt_creds")
        data_json = json.loads(google_sec)

        # Set UUID_NAMESPACE in environment
        os.environ["UUID_NAMESPACE"] = data_json.get("UUID_NAMESPACE", "")

        # Check environment (Docker or local)
        value = check_environment()
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"
        else:
            logging.info("Environment is Local.")
            profiles_dir = "/Users/roshan.bhaskhar/Documents/Alteryx/hexa-data-alteryx-workflows/src_docker/dbtcdwarehouse/profiles.yml"  # add path to the file in you local sytem here
            project_dir = "/Users/roshan.bhaskhar/Documents/Alteryx/hexa-data-alteryx-workflows/src_docker/dbtcdwarehouse/"  # add path to the file in you local sytem here

        cmd = "build"

        # Run DBT build for Piano Mart
        tag = "+tag:piano"
        tag_name = tag.split(":")[1]
        run_dbt_task(cmd, tag, profiles_dir, project_dir)

        # Run DBT build for Piano Braze Sync Mart
        tag = "+tag:piano_braze_sync_mart"
        tag_name = tag.split(":")[1]
        run_dbt_task(cmd, tag, profiles_dir, project_dir)

        logging.info("== END PIANO MAIN SCRIPT ==")
        logging.info("Total time taken: %s", datetime.now() - start_time)

        sys.exit(0)
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f"Error running dbt {cmd} {tag_name}: {e}")
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"Error running dbt {cmd} for {tag_name}: {e}",
        )
        sys.exit(1)


def run_dbt_task(cmd, tag, profiles_dir, project_dir):
    tag_name = tag.split(":")[1]

    run_dbt_build(cmd, tag, profiles_dir, project_dir)

    logging.info("dbt %s for %s completed successfully.", cmd, tag_name)

    publish_message_to_sns(
        "datateam_eks_notifications",
        f"{tag_name} dbt {cmd} tasks completed",
    )


if __name__ == "__main__":
    main()
