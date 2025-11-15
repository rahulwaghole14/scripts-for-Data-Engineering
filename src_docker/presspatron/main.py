import logging
import json
import os
import sys

from .pressPatronAPI_new import process_presspatron_data
from dbtcdwarehouse.scripts.dbt_functions import (
    update_dbt_profile,
    run_dbt_build,
    check_environment,
)

from common.aws.aws_secret import get_secret


def exit_program():
    logging.info("Exiting the program...")
    sys.exit(0)


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """

    try:
        logging.info("Processing presspatron data")
        process_presspatron_data()
        logging.info("Presspatron data processed successfully.")
        google_sec = get_secret("datateam_dbt_creds")
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
        cmd = "build"
        tag = "+tag:presspatron_api"
        logging.info("Running dbt build with presspatron")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info("presspatron dbt build completed successfully.")
        sys.exit(0)

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
