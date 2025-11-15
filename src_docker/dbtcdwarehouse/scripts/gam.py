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
from common.aws.aws_sns import publish_message_to_sns


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """

    try:
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
        tag = "+tag:gam"
        tag_name = tag.split(":")[1]
        logging.info(f"Running dbt {cmd} with {tag_name}")
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info(f"dbt {cmd} {tag_name} completed successfully.")
        cmd_test = "test"
        tag_test = "sat_adobe_replatform_test"
        logging.info(f"Running dbt {cmd_test} with {tag_test}")
        run_dbt_build(cmd_test, tag_test, profiles_dir, project_dir)
        logging.info(f"dbt {cmd_test} {tag_test} completed successfully.")
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"dbt {cmd} {tag_name} completed successfully.",
        )
        sys.exit(0)  # Exit with status code 0 to signal successful completion

    except Exception as error:
        logging.exception(
            f"An unexpected error occurred during {cmd}: {error}"
        )
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"Error running dbt {cmd} {tag_name}: {error}",
        )
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
