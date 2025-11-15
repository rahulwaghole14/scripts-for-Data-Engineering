import logging
import json
import os
import sys

from datetime import datetime

from .naviga_data_to_bq import naviga_run
from .naviga_data_targets import naviga_run_targets
from dbtcdwarehouse.scripts.dbt_functions import (
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

    try:
        logging.info("starting naviga run")
        naviga_run()
        logging.info("naviga run completed successfully.")
        logging.info("starting naviga run targets")
        naviga_run_targets()
        logging.info("naviga run targets completed successfully.")
        google_sec = get_secret("datateam_dbt_creds")
        data_json = json.loads(google_sec)
        UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
        value = check_environment()
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"
        cmd = "build"
        tag = "+tag:adw"
        logging.info(f"Running dbt {cmd} with tag {tag}")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info("dbt build completed successfully.")
        publish_message_to_sns(
            "datateam_eks_notifications",
            "naviga and dbt adw build completed successfully.",
        )
        sys.exit(0)  # Exit with status code 0 to signal successful completion

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
        publish_message_to_sns(
            "datateam_eks_notifications", f"Error running naviga task: {error}"
        )
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
