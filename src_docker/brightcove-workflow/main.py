"""
get brightcove video data into bigquery staging tables
window range update: for last 3 days incl today nzt
runs in eks pods
`project.cdw_stage.brightcove__daily_videos`
`project.cdw_stage.brightcove__daily_videos_destination`
`project.cdw_stage.brightcove__hourly_video`
"""
import datetime
import logging
import os
import sys
import json
import pytz

from common.validation.validators import validate_list_of_dicts_serialised
from common.bigquery.bigquery import (
    create_bigquery_client,
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
    generate_bq_merge_statement,
    run_bq_sql,
)
from common.logging.logger import logger, log_start, log_end
from common.aws.aws_secret import get_secret

from .validation import (
    BrightcoveResponse,
    BrightcoveDestinationPathResponse,
    BrightcoveHourlyResponse,
)
from .brightcove.get_data import (
    get_daily_data,
    get_destination_path_data,
    get_hourly_data,
)
from .brightcove.get_token import get_token

logger = logger(
    "brightcove",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

GOOGLE_DATASET = "cdw_stage"
GOOGLE_TABLE_ID = "brightcove__daily_videos"
GOOGLE_TABLE_ID_DEST_PATH = "brightcove__daily_videos_destination"
GOOGLE_TABLE_ID_HOURLY = "brightcove__hourly_video"
ACCOUNT_PROD = "hexa_production"
ACCOUNT_PLAYhexa = "playhexa"
PRIMARY_KEYS = ["video", "date", "brightcove_account"]
PRIMARY_KEYS_DEST_PATH = ["date", "destination_path", "brightcove_account"]
PRIMARY_KEYS_HOURLY = ["date", "date_hour", "brightcove_account"]
LIMIT = 2000


def get_date_to_append(days_ago):
    """Calculate the date to append based on Pacific/Auckland timezone."""
    # Define the Pacific/Auckland timezone
    nz_timezone = pytz.timezone("Pacific/Auckland")

    # Get the current date and time in the Pacific/Auckland timezone
    now_in_nz = datetime.datetime.now(nz_timezone)

    # Calculate the date to append
    date_to_append = (now_in_nz - datetime.timedelta(days=days_ago)).date()

    return date_to_append


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


def load_secrets():
    """load secrets from AWS Secrets Manager"""
    brightcove_secret = get_secret("brightcove")
    google_cred_secret = get_secret("GOOGLE_CLOUD_CRED_BASE64")
    return (
        json.loads(brightcove_secret),
        json.loads(google_cred_secret)["GOOGLE_CLOUD_CRED_BASE64"],
    )


def initialize_bigquery(google_cred, schema, schema_dest_path, schema_hourly):
    """create bq client and create table if not exists"""
    client = create_bigquery_client(google_cred)
    create_table_if_not_exists(
        client, GOOGLE_DATASET, GOOGLE_TABLE_ID, schema, "date"
    )
    create_table_if_not_exists(
        client,
        GOOGLE_DATASET,
        GOOGLE_TABLE_ID_DEST_PATH,
        schema_dest_path,
        "date",
    )
    create_table_if_not_exists(
        client,
        GOOGLE_DATASET,
        GOOGLE_TABLE_ID_HOURLY,
        schema_hourly,
        "date",
    )
    return client


def merge_temp_table_to_target(
    client, target_table, temp_table, schema, primary_keys
):
    """Merge temp table to target table"""
    merge_sql = generate_bq_merge_statement(
        target_table, temp_table, schema, primary_keys
    )
    # logging.info("merge_sql: %s", merge_sql)
    run_bq_sql(client, merge_sql)


def process_account(
    client, schema, account_id, account_name, temp_table_suffix
):
    """Process the daily data for the account"""
    temp_table = f"{GOOGLE_TABLE_ID}_{temp_table_suffix}"

    for i in range(3):  # Process the last 3 days including today

        date_to_append = get_date_to_append(i)
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
            limit=LIMIT,
        )
        all_items = items

        while len(all_items) < total:
            total, items = get_daily_data(
                token,
                date_to_append,
                brightcove_account_id=account_id,
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
                PRIMARY_KEYS,
            )


def process_dest_path_account(
    client, schema, account_id, account_name, temp_table_suffix
):
    """Process the destination path data for the account"""
    temp_table = f"{GOOGLE_TABLE_ID}_{temp_table_suffix}"

    for i in range(3):  # Process the last 3 days including today

        date_to_append = get_date_to_append(i)
        logging.info(
            "[%s: %s] date_to_append to bigquery: %s",
            account_id,
            account_name,
            date_to_append,
        )
        token = get_token()
        total, items = get_destination_path_data(
            token,
            date_to_append,
            brightcove_account_id=account_id,
            limit=LIMIT,
        )
        all_items = items

        while len(all_items) < total:
            total, items = get_destination_path_data(
                token,
                date_to_append,
                brightcove_account_id=account_id,
                limit=LIMIT,
                offset=len(all_items),
            )
            all_items.extend(items)

        # logging.info("all_items: %s", all_items)

        if all_items:
            for item in all_items:
                item["brightcove_account"] = account_name
                item["record_load_dts"] = datetime.datetime.now().isoformat()

            validate_items = validate_list_of_dicts_serialised(
                all_items, BrightcoveDestinationPathResponse
            )
            # Deduplication
            validate_items = deduplicate_dicts(
                validate_items, PRIMARY_KEYS_DEST_PATH
            )

            # logging.info("validate_items: %s", validate_items)

            load_data_to_bigquery_truncate(
                client, GOOGLE_DATASET, temp_table, validate_items, schema
            )

            merge_temp_table_to_target(
                client,
                f"{GOOGLE_DATASET}.{GOOGLE_TABLE_ID_DEST_PATH}",
                f"{GOOGLE_DATASET}.{temp_table}",
                schema,
                PRIMARY_KEYS_DEST_PATH,
            )


def process_hourly_account(
    client, schema, account_id, account_name, temp_table_suffix
):
    """Process the hourly data for the account"""
    temp_table = f"{GOOGLE_TABLE_ID}_{temp_table_suffix}"

    for i in range(3):  # Process the last 3 days including today

        date_to_append = get_date_to_append(i)
        logging.info(
            "[%s: %s] date_to_append to bigquery: %s",
            account_id,
            account_name,
            date_to_append,
        )
        token = get_token()
        total, items = get_hourly_data(
            token,
            date_to_append,
            brightcove_account_id=account_id,
            limit=LIMIT,
        )
        all_items = items

        while len(all_items) < total:
            total, items = get_hourly_data(
                token,
                date_to_append,
                brightcove_account_id=account_id,
                limit=LIMIT,
                offset=len(all_items),
            )
            all_items.extend(items)

        if all_items:
            for item in all_items:
                item["brightcove_account"] = account_name
                item["record_load_dts"] = datetime.datetime.now().isoformat()

            validate_items = validate_list_of_dicts_serialised(
                all_items, BrightcoveHourlyResponse
            )
            # Deduplication
            validate_items = deduplicate_dicts(
                validate_items, PRIMARY_KEYS_HOURLY
            )

            # logging.info("validate_items: %s", validate_items)

            load_data_to_bigquery_truncate(
                client, GOOGLE_DATASET, temp_table, validate_items, schema
            )

            merge_temp_table_to_target(
                client,
                f"{GOOGLE_DATASET}.{GOOGLE_TABLE_ID_HOURLY}",
                f"{GOOGLE_DATASET}.{temp_table}",
                schema,
                PRIMARY_KEYS_HOURLY,
            )


def query_data_from_source_table(client, project_id, dataset_id, table_id):
    """returns rowiter object i.e.
    <google.cloud.bigquery.table.RowIterator object at 0x1681b2390>
    """
    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query, location="US")  # Source region
    results = query_job.result()  # Waits for query to finish
    # Convert the RowIterator to a list of dictionaries
    logging.info("== start query results to list of dicts ==")
    df = results.to_dataframe()
    logging.info("== end query results to list of dicts ==")
    return df


def main():
    """main function"""

    try:
        log_start(logger)
        brightcove_data, google_cred = load_secrets()
        brightcove_account_id = brightcove_data["BRIGHTCOVE_ACCOUNT_ID"]
        brightcove_account_id_playhexa = brightcove_data[
            "BRIGHTCOVE_ACCOUNT_ID_PLAYhexa"
        ]

        schema_video = pydantic_model_to_bq_schema(BrightcoveResponse)
        schema_dest_path = pydantic_model_to_bq_schema(
            BrightcoveDestinationPathResponse
        )
        schema_hourly = pydantic_model_to_bq_schema(BrightcoveHourlyResponse)
        client = initialize_bigquery(
            google_cred, schema_video, schema_dest_path, schema_hourly
        )

        process_account(
            client,
            schema_video,
            brightcove_account_id,
            ACCOUNT_PROD,
            "temp_hexa_prod",
        )
        process_account(
            client,
            schema_video,
            brightcove_account_id_playhexa,
            ACCOUNT_PLAYhexa,
            "temp_playhexa",
        )
        process_dest_path_account(
            client,
            schema_dest_path,
            brightcove_account_id,
            ACCOUNT_PROD,
            "temp_hexa_prod_destpath",
        )
        process_dest_path_account(
            client,
            schema_dest_path,
            brightcove_account_id_playhexa,
            ACCOUNT_PLAYhexa,
            "temp_playhexa_destpath",
        )
        process_hourly_account(
            client,
            schema_hourly,
            brightcove_account_id,
            ACCOUNT_PROD,
            "temp_hexa_prod_hourly",
        )
        process_hourly_account(
            client,
            schema_hourly,
            brightcove_account_id_playhexa,
            ACCOUNT_PLAYhexa,
            "temp_playhexa_hourly",
        )

        log_end(logger)
    except Exception as error:  # pylint: disable=broad-except
        logger.error("Script failed with exception: %s", error)
        sys.exit(1)


if __name__ == "__main__":
    main()
