""" main """
# pylint: disable=import-error

import os
import json
import logging
import time
from common.logging.logger import logger
from common.bigquery.bigquery import (
    create_bigquery_client,
    return_bq_sql_dict,
    run_bq_sql,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
)
from common.aws.aws_secret import get_secret
from .ppid_generator import generate_ppids_concurrently, generate_ppid_js

logger(
    "idm_api__user_profiles_to_bigquery",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Load the environment variables from .env file
# load_dotenv()


DATASET_ID = "cdw_stage"
TABLE_ID = "account_management__user_profiles"
TEMP_TABLE_ID = "temp_backfillppid_" + TABLE_ID

data_google = get_secret("datateam_google_cred_prod_base64")
data = json.loads(data_google)

# google credentials
google_cred = data["GOOGLE_CLOUD_CRED_BASE64"]


if __name__ == "__main__":

    try:
        # start timer
        start_time = time.time()
        # initialise bigquery client
        client = create_bigquery_client(google_cred)

        # get data from bigquery
        # all id TABLE_ID where ppid is null
        # prepare query:
        query = f"""
        SELECT id
        FROM {DATASET_ID}.{TABLE_ID}
        WHERE ppid IS NULL
        """

        # testing
        # -- WHERE ppid IS NULL
        # -- WHERE id IN (1002322, 461097, 843902, 370703, 10853294)

        # run query
        result = return_bq_sql_dict(client, query)
        # logging.info(f"Data: {result[0]}")

        # Determine the number of CPUs
        num_cpus = os.cpu_count()
        logging.info(f"Number of CPUs: {num_cpus}")

        # Set max_workers to 25% of the available CPUs
        max_workers = max(1, num_cpus // 2)
        logging.info(f"Using {max_workers} threads for ThreadPoolExecutor")

        # # show js code output
        # for user in result:
        #     user["ppid_js"] = generate_ppid_js(str(user["id"]))
        #     logging.info(
        #         f"js Generated PPID for user: {user['id']}, {user['ppid_js']}"
        #     )

        # generate ppids concurrently
        result = generate_ppids_concurrently(result, max_workers)

        # logging.info(f"Data udpated: {result[0]}")

        # create temp table with id and ppid
        schema = [
            {"name": "id", "type": "INTEGER"},
            {"name": "ppid", "type": "STRING"},
        ]

        # generate merge sql
        primary_keys = ["id"]
        TARGET_DATASET_TABLE = DATASET_ID + "." + TABLE_ID
        TEMP_DATASET_TABLE = DATASET_ID + "." + TEMP_TABLE_ID

        create_table_if_not_exists(client, DATASET_ID, TEMP_TABLE_ID, schema)
        # write data to bigquery
        load_data_to_bigquery_truncate(
            client, DATASET_ID, TEMP_TABLE_ID, result, schema
        )

        merge_sql = f"""
        MERGE `{TARGET_DATASET_TABLE}` AS TARGET
        USING `{TEMP_DATASET_TABLE}` AS TEMP
        ON TARGET.id = TEMP.id
        WHEN MATCHED THEN UPDATE SET ppid = TEMP.ppid
        """

        # run the merge sql
        run_bq_sql(client, merge_sql)

    except Exception as error:
        logging.exception("An unexpected error occurred: %s", error)
