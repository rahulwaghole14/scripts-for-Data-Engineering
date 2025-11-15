import datetime
import logging
import os
import time
import json
from dotenv import load_dotenv


from a_common.validation.validators import validate_list_of_dicts_serialised
from a_common.bigquery.bigquery import (
    create_bigquery_client,
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
    generate_bq_merge_statement,
    run_bq_sql,
)
from a_common.logging.logger import logger

from .validation import BrightcoveResponse
from .brightcove.get_data import get_daily_data
from .brightcove.get_token import get_token

logger(
    "brightcove",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Load the environment variables from .env file
load_dotenv()

GOOGLE_DATASET = "cdw_stage"
GOOGLE_TABLE_ID = "brightcove__daily_videos"
ACCOUNT_PROD = "hexa_production"
ACCOUNT_PLAYhexa = "playhexa"
PRIMARY_KEYS = ["video", "date", "brightcove_account"]
LIMIT = 2000


def deduplicate_dicts(dict_list, pks):
    """
    Deduplicate a list of dictionaries based on specified primary keys.

    Parameters:
    - dict_list: List of dictionaries to deduplicate.
    - pks: List of primary keys to use for deduplication.

    Returns:
    - A list of deduplicated dictionaries.
    """
    seen = set()
    unique_list = []

    for d in dict_list:
        # Create a tuple of primary key values for hashing
        pk_tuple = tuple(d[pk] for pk in pks)

        if pk_tuple not in seen:
            seen.add(pk_tuple)
            unique_list.append(d)

    return unique_list


def initialize_bigquery(google_cred_val):
    """
    Initialize the BigQuery client and create the target table if it doesn't exist.
    """
    client = create_bigquery_client(google_cred_val)
    schema = pydantic_model_to_bq_schema(BrightcoveResponse)
    create_table_if_not_exists(client, GOOGLE_DATASET, GOOGLE_TABLE_ID, schema)
    return client, schema


def merge_temp_table_to_target(
    client, target_table, temp_table, schema, primary_keys
):
    merge_sql = generate_bq_merge_statement(
        target_table, temp_table, schema, primary_keys
    )
    run_bq_sql(client, merge_sql)


def process_account(
    client, schema, account_id, account_name, temp_table_suffix
):
    temp_table = f"{GOOGLE_TABLE_ID}_{temp_table_suffix}"
    create_table_if_not_exists(client, GOOGLE_DATASET, temp_table, schema)

    for i in range(2):  # Process the last 2 days
        date_to_append = datetime.date.today() - datetime.timedelta(days=i + 1)
        logging.info(
            "[%s: %s] date_to_append to bigquery: %s",
            account_id,
            account_name,
            date_to_append,
        )
        token = get_token()
        total, items = get_daily_data(
            token,
            date_to_append,
            brightcove_account_id=account_id,
            account_name=account_name,
            limit=LIMIT,
        )
        all_items = items

        while len(all_items) < total:
            total, items = get_daily_data(
                token,
                date_to_append,
                brightcove_account_id=account_id,
                account_name=account_name,
                limit=LIMIT,
                offset=len(all_items),
            )
            all_items.extend(items)

        if all_items:
            for item in all_items:
                item["brightcove_account"] = account_name
                item["record_load_dts"] = datetime.datetime.now().isoformat()

            validate_items = validate_list_of_dicts_serialised(
                all_items, BrightcoveResponse
            )
            # Deduplication
            validate_items = deduplicate_dicts(validate_items, PRIMARY_KEYS)

            load_data_to_bigquery_truncate(
                client, GOOGLE_DATASET, temp_table, validate_items, schema
            )
            merge_temp_table_to_target(
                client,
                f"{GOOGLE_DATASET}.{GOOGLE_TABLE_ID}",
                f"{GOOGLE_DATASET}.{temp_table}",
                schema,
                ["video", "date", "brightcove_account"],
            )


if __name__ == "__main__":
    start = time.time()
    logging.info(
        "Starting brightcove__videos_to_bigquery at %s",
        str(datetime.datetime.now()),
    )

    google_cred = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")
    brightcove_account_id = os.environ.get("BRIGHTCOVE_ACCOUNT_ID")
    brightcove_account_id_playhexa = os.environ.get(
        "BRIGHTCOVE_ACCOUNT_ID_PLAYhexa"
    )

    client, schema = initialize_bigquery(google_cred)

    process_account(
        client, schema, brightcove_account_id, ACCOUNT_PROD, "temp_hexa_prod"
    )
    process_account(
        client,
        schema,
        brightcove_account_id_playhexa,
        ACCOUNT_PLAYhexa,
        "temp_playhexa",
    )

    logging.info(
        "Finished brightcove__videos_to_bigquery at %s",
        str(datetime.datetime.now()),
    )
    end = time.time()
    logging.info("Total time: %s seconds", str(end - start))
