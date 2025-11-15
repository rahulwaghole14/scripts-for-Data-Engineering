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
# end_date = today_date - timedelta(days=179)
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


def fetch_data_from_api(session, endpoint, path, user, pwd):
    """fetch list of content ids and append to master json object"""
    master_data = []
    offset = 0  # increments by limit to retrieve pages

    while True:
        offsetstr = str(offset)
        api_query = (
            f"?limit=10&offset={offsetstr}&{ELASTISEARCHPARAMS}&q=type=in=(article);"
            f"moderation_state=in=(published);published_date=ge={start_date_str};published_date=le={end_date_str}"
            # f"moderation_state=in=(published);created=ge={start_date_str};created=le={end_date_str}"
        )
        # logging.info(
        #     "Getting content ids for range: %s to %s limit=%soffset=%s",
        #     start_date_str,
        #     end_date_str,
        #     LIMIT,
        #     offsetstr,
        # )

        try:
            result = get_content(session, endpoint, path, api_query, user, pwd)

            if result.status_code == 200:
                data = result.json()

                if data == "[]" or not data:
                    break

                master_data.extend(data)
                offset += LIMIT
            else:
                logging.error("Failed to fetch data: %s", result.status_code)
                break
        except Exception as erros:  # pylint: disable=broad-except
            logging.error("Exception occurred while fetching data: %s", erros)
            break
    logging.info(
        "fetch_data_from_api(): Total number of items fetched: %s",
        len(master_data),
    )

    return master_data


def process_content_items(  # pylint: disable=too-many-arguments
    session, endpoint, path_id, user, pwd, content_items
):
    """get each content id data from api"""
    all_content_metadata = []
    for item in content_items:
        try:
            main_pub_key = item.mainPublicationChannel.key
            item_id = item.id

            if not main_pub_key:
                logging.warning(
                    "No mainPublicationChannel key found for item ID: %s",
                    item_id,
                )
                continue
            result_id = get_content_metadata(
                session, endpoint, path_id, item_id, main_pub_key, user, pwd
            )

            if result_id.status_code == 200:
                # logging.info(
                #     "contentid: %s mainpubchannel: %s", item_id, main_pub_key
                # )
                content_meta = parse_content_meta(result_id.json())
                all_content_metadata.append(content_meta)
            else:
                logging.error(
                    "Failed to fetch metadata for content ID %s: %s",
                    item.id,
                    result_id.status_code,
                )
        except Exception as errors:  # pylint: disable=broad-except
            logging.info(
                "Exception occurred while processing item in process_content_items(): %s: %s",
                item.id,
                errors,
            )

    return all_content_metadata


def main():
    """main flow"""
    dataset_id = "cdw_stage"
    table_id = "drupal__articles"
    temp_table_id = "temp_drupal__articles"
    session = init_session()
    data = fetch_data_from_api(
        session, API_ENDPOINT, API_PATH_CONTENT, API_USR, API_PW
    )

    if data:
        content_items = parse_content(data)

        logging.info("content items to retrieve: %s", len(content_items))

        all_content_metadata = process_content_items(
            session, API_ENDPOINT, API_PATH_ID, API_USR, API_PW, content_items
        )

        content_data_dicts = convert_to_dicts(all_content_metadata)
        df = pd.DataFrame(content_data_dicts)
        # deduplication
        df = df.drop_duplicates(subset=["id"])

        client = create_bigquery_client(google_cred)

        # load_data_to_bigquery(client, df, dataset_id, table_id, BQ_SCHEMA)

        create_table_if_not_exists(
            client, dataset_id, temp_table_id, BQ_SCHEMA
        )
        create_table_if_not_exists(client, dataset_id, table_id, BQ_SCHEMA)

        load_data_to_bigquery(client, df, dataset_id, temp_table_id, BQ_SCHEMA)

        primary_keys = ["id"]
        target_dataset_table = dataset_id + "." + table_id
        temp_dataset_table = dataset_id + "." + temp_table_id

        merge_sql = generate_bq_merge_statement(
            target_dataset_table, temp_dataset_table, BQ_SCHEMA, primary_keys
        )

        run_bq_sql(client, merge_sql)

    session.close()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)  # Successful completion
    except Exception as err:  # pylint: disable=broad-except
        logging.error("Error encountered: %s", err)
        sys.exit(1)
