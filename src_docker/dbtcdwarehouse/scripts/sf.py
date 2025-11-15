"""
source freshness check
"""

import logging
import json
import os
import sys
from datetime import datetime
from common.aws.aws_secret import get_secret
from common.logging.logger import logger, log_start, log_end
from common.bigquery.bigquery import (
    create_bigquery_client,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
)
from .dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_freshness,
)

logger = logger(
    "source-freshness",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


def main():
    """
    This is the main entry point for the script.
    It calls the run_dbt_build_with_sentiment_tag_and_env function to execute the dbt build process.
    """

    log_start(logger)

    try:
        google_sec = get_secret("datateam_dbt_creds")
        google_cred_secret = get_secret("GOOGLE_CLOUD_CRED_BASE64")
        google_cred = json.loads(google_cred_secret)[
            "GOOGLE_CLOUD_CRED_BASE64"
        ]

        data_json = json.loads(google_sec)
        uuid_namespace = data_json["UUID_NAMESPACE"]
        os.environ["UUID_NAMESPACE"] = uuid_namespace
        value = check_environment()
        if value == "Docker":
            logging.info("Environment is Docker.")
            update_dbt_profile(google_sec)
            profiles_dir = "/usr/src/app/dbtcdwarehouse"
            project_dir = "/usr/src/app/dbtcdwarehouse"

        data = run_dbt_freshness(profiles_dir, project_dir)

        # for each dict in the list, append the current timestamp for record_load_dts
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for record in data:
            record["record_load_dts"] = current_time

        # Create a BigQuery client
        client = create_bigquery_client(google_cred)

        # Define the BigQuery table schema
        schema = [
            {"name": "line", "type": "STRING"},
            {"name": "record_load_dts", "type": "TIMESTAMP"},
        ]

        # Create the BigQuery table if it does not exist
        create_table_if_not_exists(
            client, "cdw_stage", "source_freshness", schema
        )

        # load the data to BigQuery
        load_data_to_bigquery_truncate(
            client, "cdw_stage", "source_freshness", data, schema
        )

        log_end(logger)
        sys.exit(0)  # Exit with status code 0 to signal success

    except Exception as error:  # pylint: disable=broad-except
        logging.exception("An unexpected error occurred: %s", error)
        sys.exit(1)  # Exit with status code 1 to signal an error


if __name__ == "__main__":
    main()
