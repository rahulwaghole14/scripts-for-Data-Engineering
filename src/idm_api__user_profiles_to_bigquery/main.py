""" main """
# pylint: disable=import-error

from datetime import datetime, timedelta, UTC
import os
import json
import logging

from dotenv import load_dotenv

from a_common.validation.validators import validate_list_of_dicts_serialised
from a_common.logging.logger import logger

from a_common.marketing.uuid_v5 import generate_uuid_v5
from a_common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    create_bigquery_client,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
    # create_or_update_table_schema,
    generate_bq_merge_statement,
    run_bq_sql,
)

from .api_idm.get_batch_users import batch_get_users
from .validation import UserProfile
from .ppid_generator import generate_ppid, generate_ppids_concurrently

logger(
    "idm_api__user_profiles_to_bigquery",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Load the environment variables from .env file
load_dotenv()

# filter params
now = datetime.now(UTC)
# delta = timedelta(hours=48)
delta = timedelta(hours=24 * 4)
# delta = timedelta(hours=1)
before = now - delta
timestamp = before.strftime("%Y-%m-%dT%H:%M:%SZ")

# set mode 0 = get data between dates,
# 1 = get all data from date,
# 2 = get data between dates and filter on id,
MODE = 1

# filter on ids
# id le "93888" and id ge "83888"
START_ID = 400000
END_ID = 500000

# START_DATE = timestamp # 24 hours before now utc
END_DATE = None
START_DATE = timestamp
COUNT = 100

DATASET_ID = "cdw_stage"
TABLE_ID = "account_management__user_profiles"
TEMP_TABLE_ID = "temp_" + TABLE_ID

# uuid namespace
UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")

# google credentials
google_cred = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")

# API endpoint and credentials
# API_URL = "https://account-management-stage.staging.nebula-drupal.hexa.co.nz"
API_URL = "https://accounts.hexa.co.nz"
username = os.environ.get("API_USERNAME")
password = os.environ.get("API_PASSWORD")
apiauth = os.environ.get("API_AUTH")

MAX_RETRIES = 5
RETRY_DELAY = 15.0  # delay between retries in seconds

if __name__ == "__main__":

    # read data from api
    data = batch_get_users(
        API_URL,
        apiauth,
        COUNT,
        START_ID,
        END_ID,
        START_DATE,
        END_DATE,
        MODE,
    )

    # validate data
    validated_data = validate_list_of_dicts_serialised(data, UserProfile)

    # generate marketing ids
    for user in validated_data:
        user["marketing_id"] = generate_uuid_v5(user["id"], UUID_NAMESPACE)
        # when email is primary, use it as marketing_id_email
        if user["emails"][0]["primary"]:
            user["marketing_id_email"] = generate_uuid_v5(
                user["emails"][0]["value"], UUID_NAMESPACE
            )

    # generate ppids concurrently
    validated_data = generate_ppids_concurrently(validated_data)

    # create bigquery schema
    schema = pydantic_model_to_bq_schema(UserProfile)

    # initialise bigquery client
    client = create_bigquery_client(google_cred)

    # create table if not exists
    create_table_if_not_exists(client, DATASET_ID, TEMP_TABLE_ID, schema)
    create_table_if_not_exists(
        client,
        DATASET_ID,
        TABLE_ID,
        schema,
    )
    # # # if schema needs to be updated, run: (instead of above line)
    # create_or_update_table_schema(client, DATASET_ID, TEMP_TABLE_ID, schema)
    # create_or_update_table_schema(client, DATASET_ID, TABLE_ID, schema)

    # remove any duplicates in validated data based on id field
    seen_ids = set()
    new_validated_data = []
    for user in validated_data:
        if user["id"] not in seen_ids:
            new_validated_data.append(user)
            seen_ids.add(user["id"])
    validated_data = new_validated_data

    # set record_loaded_dts field
    for user in validated_data:
        user["record_loaded_dts"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    # write data to bigquery
    load_data_to_bigquery_truncate(
        client, DATASET_ID, TEMP_TABLE_ID, new_validated_data, schema
    )
    primary_keys = ["id"]
    TARGET_DATASET_TABLE = DATASET_ID + "." + TABLE_ID
    TEMP_DATASET_TABLE = DATASET_ID + "." + TEMP_TABLE_ID

    merge_sql = generate_bq_merge_statement(
        TARGET_DATASET_TABLE, TEMP_DATASET_TABLE, schema, primary_keys
    )

    run_bq_sql(client, merge_sql)
