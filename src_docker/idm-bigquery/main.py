""" main """
# pylint: disable=import-error

import json
import logging
import os
import warnings
from datetime import datetime, timedelta, UTC

from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns
from common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    create_bigquery_client,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
    create_or_update_table_schema,
    generate_bq_merge_statement,
    run_bq_sql,
)
from common.logging.logger import logger, log_start, log_end
from common.marketing.uuid_v5 import generate_uuid_v5
from common.validation.validators import validate_list_of_dicts_serialised
# from .logger import logger
from dbtcdwarehouse.scripts.dbt_functions import (
    check_environment,
    update_dbt_profile,
    run_dbt_build,
)

from .api_idm.get_batch_users import batch_get_users
from .ppid_generator import generate_ppids_concurrently
from .validation import UserProfile

logger = logger(
    "idm_api__user_profiles_to_bigquery",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# filter params
now = datetime.now(UTC)
delta = timedelta(hours=48)
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

data_google = get_secret("datateam_google_cred_prod_base64")
data = json.loads(data_google)
data_idm = get_secret("datateam_idm_and_composer_api")
data_idm_json = json.loads(data_idm)

# google credentials
google_cred = data["GOOGLE_CLOUD_CRED_BASE64"]

# uuid namespace
UUID_NAMESPACE = data_idm_json["UUID_NAMESPACE"]

# API endpoint and credentials
# API_URL = "https://account-management-uat.staging.nebula-drupal.hexa.co.nz"
API_URL = "https://accounts.hexa.co.nz"
username = data_idm_json["API_USERNAME"]
password = data_idm_json["API_PASSWORD"]
apiauth = data_idm_json["API_AUTH"]

MAX_RETRIES = 5
RETRY_DELAY = 15.0  # delay between retries in seconds


def main():
    """main function"""

    try:
        log_start(logger)
        # read data from api
        dataresponse = batch_get_users(
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
        validated_data = validate_list_of_dicts_serialised(
            dataresponse, UserProfile, "id"
        )

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

        # initialise bigquery client - dev
        # google_sec = get_secret("datateam_dbt_cred_dev")
        # google_cred = json.loads(google_sec)
        # client = create_bigquery_client_dev(google_cred)

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
        # run schema update: add new fields to the table only if they are not already present
        create_or_update_table_schema(
            client, DATASET_ID, TEMP_TABLE_ID, schema
        )
        create_or_update_table_schema(client, DATASET_ID, TABLE_ID, schema)

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
        target_dataset_table = DATASET_ID + "." + TABLE_ID
        temp_dataset_table = DATASET_ID + "." + TEMP_TABLE_ID

        merge_sql = generate_bq_merge_statement(
            target_dataset_table, temp_dataset_table, schema, primary_keys
        )

        run_bq_sql(client, merge_sql)
    except Exception as e:
        logger.error(e)
        raise e

def run_dbt_braze_sync_mart():
    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials "
        "from Google Cloud SDK",
    )

    try:
        logging.info("Running dbt BRAZE mart")
        #google_sec = get_secret("datateam_dbt_cred_dev")
        google_sec = get_secret("datateam_dbt_creds")
        data_json = json.loads(google_sec)
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
        tag = "+tag:braze_sync_mart"
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
    log_start(logger)
    main()
    run_dbt_braze_sync_mart()
    log_end(logger)

