import logging
import requests
import json
import os
import sys
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential
from .dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)
from common.aws.aws_secret import get_secret
from naviga.naviga_data_to_bq import naviga_run


try:
    naviga_run()
    logging.info("Naviga data to BQ run successfully.")
    data = get_secret("datateam_naviga_api")
    data_json = json.loads(data)
    UUID_NAMESPACE = data_json["UUID_NAMESPACE"]
    os.environ["UUID_NAMESPACE"] = UUID_NAMESPACE
    cmd = "build"
    tag = "+tag:adw"
    value = check_environment()
    if value == "Docker":
        logging.info("Environment is Docker.")
        update_dbt_profile()
        profiles_dir = "/usr/src/app/dbtcdwarehouse"
        project_dir = "/usr/src/app/dbtcdwarehouse"
    # run_dbt_build(cmd, tag, profiles_dir, project_dir)
    logging.info(f"dbt {cmd} completed successfully.")

except Exception as error:
    logging.exception("An unexpected error occurred: %s", error)
    sys.exit(1)  # Exit with status code 1 to signal an error
