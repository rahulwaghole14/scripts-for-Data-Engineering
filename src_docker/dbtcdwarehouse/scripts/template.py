import logging
import json
import os
import sys
from .dbt_functions import (
    check_environment,
    run_dbt_freshness,
    update_dbt_profile,
    run_dbt_build,
)
from common.aws.aws_secret import get_secret


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """
    logging.info(
        "Starting dbt build with sentiment tag and environment setup."
    )

    try:
        data = get_secret("datateam_piano_cred")
        data_json = json.loads(data)
        UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
        cmd = "build"
        tag = "+tag:sentiment"
        value = check_environment()
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile()
        # run_dbt_freshness()
        run_dbt_build(cmd, tag)
        logging.info("Running dbt build with sentiment tag.")
        value = check_environment()
        logging.info("location is %s", value)
        logging.info("dbt build completed successfully.")

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
