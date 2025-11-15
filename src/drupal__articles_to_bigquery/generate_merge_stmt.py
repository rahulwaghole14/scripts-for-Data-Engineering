""" get data from drupal api for articles """
import os
import sys
import logging
from typing import List
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from a_common.bigquery.bigquery import (
    create_table_if_not_exists,
    generate_bq_merge_statement,
    run_bq_sql,
    create_bigquery_client,
)

from .drupalapi.getarticles import (
    get_content,
    get_content_metadata,
    init_session,
)
from .bigquery.bigquery_get_and_load import (
    load_data_to_bigquery,
)
from .logger import logger
from .data_validation import (
    ContentItemMeta,
    BQ_SCHEMA,
    parse_content,
    parse_content_meta,
)

logger("drupalapi")
load_dotenv()

API_PATH_CONTENT = "content"
API_PATH_ID = "display/content/"

# prod config
today_date = datetime.now()
start_date = today_date - timedelta(days=15)
# end_date = today_date - timedelta(days=13)
end_date = today_date + timedelta(days=1)
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
API_ENDPOINT = os.environ.get("DRUPAL_CONTENT_API_PRD")
API_USR = os.environ.get("DRUPAL_CONTENT_API_PRD_USR")
API_PW = os.environ.get("DRUPAL_CONTENT_API_PRD_PW")

# backfill/test config
# API_ENDPOINT = os.environ.get("DRUPAL_CONTENT_API_UAT")
# API_USR = os.environ.get("DRUPAL_CONTENT_API_UAT_USR")
# API_PW = os.environ.get("DRUPAL_CONTENT_API_UAT_PW")
# # start_date_str = '2023-04-01'
# # end_date_str = '2023-04-01'
# start_date_str = '2024-01-26'
# end_date_str = '2024-01-31'

LIMIT = 10  # advised not to go beyond 10 as limit
ELASTISEARCHPARAMS = "scoring_enabled=false&sortBy=created&sortOrder=ASC"

google_cred = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")


def convert_to_dicts(content_metadata: List[ContentItemMeta]) -> List[dict]:
    """convert data into dictionary"""
    return [item.model_dump(by_alias=True) for item in content_metadata]


def main():
    """main flow"""
    dataset_id = "cdw_stage"
    table_id = "drupal__articles"
    temp_table_id = "temp_drupal__articles"

    # logging.info("content items to retrieve: %s", len(content_items))
    # client = create_bigquery_client(google_cred)
    # # load_data_to_bigquery(client, df, dataset_id, table_id, BQ_SCHEMA)

    # create_table_if_not_exists(
    #     client, dataset_id, temp_table_id, BQ_SCHEMA
    # )
    # create_table_if_not_exists(client, dataset_id, table_id, BQ_SCHEMA)

    # load_data_to_bigquery(client, df, dataset_id, temp_table_id, BQ_SCHEMA)

    primary_keys = ["id"]
    target_dataset_table = dataset_id + "." + table_id
    temp_dataset_table = dataset_id + "." + temp_table_id

    merge_sql = generate_bq_merge_statement(
        target_dataset_table, temp_dataset_table, BQ_SCHEMA, primary_keys
    )

    print(merge_sql)
    # run_bq_sql(client, merge_sql)


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)  # Successful completion
    except Exception as err:  # pylint: disable=broad-except
        logging.error("Error encountered: %s", err)
        sys.exit(1)
