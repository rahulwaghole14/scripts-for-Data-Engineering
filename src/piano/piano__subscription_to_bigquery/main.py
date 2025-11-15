"""
pianos subs to bq
"""

import os
import logging
import time
from dotenv import load_dotenv

# from a_common.bigquery import
from a_common.marketing.uuid_v5 import generate_uuid_v5
from a_common.logging.logger import logger
from a_common.validation.validators import validate_list_of_dicts
from a_common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    # create_or_update_table_schema,
    load_data_to_bigquery_truncate,
    run_bq_sql,
    generate_bq_merge_statement,
    create_bigquery_client,
)

from .piano import fetch_all_data
from .validator import UserSubscriptionListItem

logger(
    "piano", os.path.join(os.path.dirname(os.path.realpath(__file__)), "log")
)

load_dotenv()

UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")
endpoint = os.getenv("PIANO_ENDPOINT")
api_request = os.getenv("PIANO_API_REQUEST")
app_ids = os.getenv("PIANO_APP_IDS").split(",")
app_names = os.getenv("PIANO_APP_NAMES").split(",")
app_tokens = os.getenv("PIANO_APP_TOKENS").split(",")
google_base64 = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")

# refreshing on mondays i.e. full weekend sat, sun, mon
timestamp = int(time.time()) - (60 * 60 * 24 * 14)

# get timestamp for last year start
# timestamp = int(time.time()) - (60 * 60 * 24 * 365)

# convert to string
TIMESTAMP = str(timestamp)
logging.info("timestamp now minus 48 hours: %s", TIMESTAMP)
REC_LIMIT = 200

if __name__ == "__main__":

    schema = pydantic_model_to_bq_schema(UserSubscriptionListItem)
    # logging.info(schema)
    DATASET = "cdw_stage"
    TEMP_TABLE = "piano__temp_subscriptions"
    TABLE = "piano__subscriptions"
    client = create_bigquery_client(google_base64)
    create_table_if_not_exists(client, DATASET, TEMP_TABLE, schema)
    create_table_if_not_exists(client, DATASET, TABLE, schema)
    # if schema needs to be updated, run: (instead of above line)
    # create_or_update_table_schema(client, DATASET, TABLE, schema)

    for app_id, app_name, app_token in zip(app_ids, app_names, app_tokens):

        try:
            # Call the functions to process the data for the current app_id, app_name, and app_token
            data = fetch_all_data(
                endpoint, api_request, app_id, app_token, TIMESTAMP, REC_LIMIT
            )
            logging.info(
                "got data for %s, length of data: %s", app_name, len(data)
            )
            data_validated = validate_list_of_dicts(
                data, UserSubscriptionListItem
            )

            for user in data_validated:
                user["marketing_id"] = generate_uuid_v5(
                    user["user"]["uid"], UUID_NAMESPACE
                )

            # load data to bigquery
            load_data_to_bigquery_truncate(
                client, DATASET, TEMP_TABLE, data_validated, schema
            )
            primary_keys = ["subscription_id"]
            TARGET_DATASET_TABLE = DATASET + "." + TABLE
            TEMP_DATASET_TABLE = DATASET + "." + TEMP_TABLE

            merge_sql = generate_bq_merge_statement(
                TARGET_DATASET_TABLE, TEMP_DATASET_TABLE, schema, primary_keys
            )

            run_bq_sql(client, merge_sql)

        except Exception as error:  # pylint: disable=broad-except
            logging.info("error: %s", error)
