"""
pianos subs to bq
"""

import os
import logging
import time
import json
import pytz

from datetime import datetime

# from dotenv import load_dotenv

# from common.bigquery import
from common.marketing.uuid_v5 import generate_uuid_v5
from common.logging.logger import logger
from common.validation.validators import validate_list_of_dicts
from common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    # create_or_update_table_schema,
    load_data_to_bigquery_truncate,
    run_bq_sql,
    generate_bq_merge_statement,
    create_bigquery_client,
)
from common.aws.aws_secret import get_secret

from .piano import fetch_all_data
from .validator import UserSubscriptionListItem

logger(
    "piano", os.path.join(os.path.dirname(os.path.realpath(__file__)), "log")
)

# load_dotenv()
piano_secret = get_secret("datateam_piano_cred")
data = json.loads(piano_secret)
endpoint = data["PIANO_ENDPOINT"]
api_request = data["PIANO_API_REQUEST"]
app_ids = data["PIANO_APP_IDS"]
app_names = data["PIANO_APP_NAMES"]
app_tokens = data["PIANO_APP_TOKENS"]
UUID_NAMESPACE = data["UUID_NAMESPACE"]
google_cred_secret = get_secret("datateam_google_cred_prod_base64")
data_google = json.loads(google_cred_secret)
google_base64 = data_google["GOOGLE_CLOUD_CRED_BASE64"]

# Define the NZT timezone
nztime = pytz.timezone("Pacific/Auckland")

# Get the current time in NZT
nz_now = datetime.now(nztime)

# refreshing on mondays i.e. full weekend sat, sun, mon
# start from 7 days ago.
timestamp = int(nz_now.timestamp()) - (60 * 60 * 24 * 7)

# convert to string
TIMESTAMP = str(timestamp)
logging.info("timestamp now minus 48 hours: %s", TIMESTAMP)
REC_LIMIT = 200


def subbq():

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
