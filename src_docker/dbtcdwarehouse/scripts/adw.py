import logging
import json
import os
import sys
from .dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)
from common.aws.aws_secret import get_secret


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """
    logging.info("Starting dbt source freshness check")

    try:
        google_sec = get_secret("datateam_dbt_cred_dev")
        data_json = json.loads(google_sec)
        UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
        value = check_environment()
        # profiles_dir = "/Users/roshan.bhaskhar/Documents/Alteryx/hexa-data-alteryx-workflows/src_docker/dbtcdwarehouse"
        # project_dir = "/src_docker/dbtcdwarehouse"
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"
        cmd = "test"
        tag = "+tag:adw"
        logging.info("Running dbt build with adw")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info(f"dbt {cmd} completed successfully.")
        sys.exit(0)  # Exit with status code 0 to signal success

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
