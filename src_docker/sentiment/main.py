import warnings
import logging
import json
import os
from .parsely import parsely_run
from .drupal.drupal import drupal_run
from .logger import logger
from dbtcdwarehouse.scripts.dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)
from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns

# Setup logging


def main():
    logger("sentiment")
    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials "
        "from Google Cloud SDK",
    )

    try:
        logging.info("Running drupal task")

        drupal_run()
        logging.info("Drupal task completed")
    except Exception as e:
        publish_message_to_sns(
            "datateam_eks_notifications", f"Error running drupal task: {e}"
        )
        logging.error(f"Error running drupal task: {e}")

    try:
        logging.info("Running parsely task")
        parsely_run()
        logging.info("Parsely task completed")
    except Exception as e:
        publish_message_to_sns(
            "datateam_eks_notifications", f"Error running parsely task: {e}"
        )
        logging.error(f"Error running parsely task: {e}")

    try:
        logging.info("Running dbt sentiment mart")
        google_sec = get_secret("datateam_dbt_creds")
        data_json = json.loads(google_sec)
        logging.info(data_json)
        UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
        value = check_environment()
        # to run dbt locally uncomment the below line
        # profiles_dir = "/Users/roshan.bhaskhar/Documents/Alteryx/hexa-data-alteryx-workflows/src_docker/dbtcdwarehouse/profiles.yml"
        # project_dir = "/Users/roshan.bhaskhar/Development/hexa/hexa-alteryx-workflows/src_docker/dbtcdwarehouse"
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"
        cmd = "build"
        tag = "+tag:sentiment"
        tag_name = tag.split(":")[1]
        run_dbt_build(cmd, tag, profiles_dir, project_dir)
        logging.info(f"dbt {cmd} {tag_name} mart completed")
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"dbt {cmd} {tag_name} mart completed",
        )
    except Exception as e:
        publish_message_to_sns(
            "datateam_eks_notifications",
            f"Error running dbt {cmd} {tag_name}: {e}",
        )
        logging.error(f"Error running dbt {cmd} {tag_name}: {e}")


if __name__ == "__main__":
    main()
