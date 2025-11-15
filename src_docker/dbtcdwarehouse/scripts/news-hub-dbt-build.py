import logging
import json
import os
from pdb import run
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
        data = get_secret("datateam_piano_cred")
        data_json = json.loads(data)
        UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE

        if check_environment() == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile()
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"

        cmd = "build"
        tag = "+tag:qualtrics_news_hub"
        logging.info(f"Running dbt build with command '{cmd}' and tag '{tag}'")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info("dbt build completed successfully.")

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
