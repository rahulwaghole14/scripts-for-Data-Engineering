import logging
import json
import os
import sys
from zoneinfo import ZoneInfo
from datetime import datetime
from .dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)
from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        # Current time in NZT
        nzt = ZoneInfo("Pacific/Auckland")
        current_time_nzt = datetime.now(nzt)
        # Target date in NZT
        target_date_nzt = datetime(2024, 12, 31, tzinfo=nzt)

        logging.info(f"Current time in NZT: {current_time_nzt}")
        logging.info(f"Target date in NZT: {target_date_nzt}")

        # Exit if the current date is after the target date
        if current_time_nzt > target_date_nzt:
            logging.info("Date is after 31/12/2024 in NZT. Exiting script.")
            sys.exit(0)

        # Fetch Google secrets
        google_sec = get_secret("datateam_dbt_creds")
        data_json = json.loads(google_sec)

        # Set UUID_NAMESPACE environment variable
        UUID_NAMESPACE = data_json.get("UUID_NAMESPACE")
        if not UUID_NAMESPACE:
            raise ValueError("UUID_NAMESPACE is missing in secrets")
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE

        # Check environment and set directories
        value = check_environment()
        profiles_dir = "/default/profiles/dir"
        project_dir = "/default/project/dir"

        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"

        # Run dbt build command
        cmd = "build"
        tag = "+tag:adobe"
        tag_name = tag.split(":")[1]

        logging.info(f"Running dbt {cmd} with {tag_name}")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info(f"dbt {cmd} {tag_name} completed successfully.")
        # Notify completion
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"dbt {cmd} {tag_name} completed successfully.",
        )

        sys.exit(0)  # Exit with status code 0 to signal successful completion

    except Exception as error:
        logging.exception(f"An unexpected error occurred during execution: {error}")
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"Error running dbt: {error}",
        )
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
