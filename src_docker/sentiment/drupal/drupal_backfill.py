# bug/CD-1265-backfill-missing-article-ids-for-drupal-dpa-into-bigquery

""" get data from drupal api for articles """
import json
import sys
import logging
from typing import List

import pandas as pd
from dotenv import load_dotenv

from common.bigquery.bigquery import (
    create_table_if_not_exists,
    generate_bq_merge_statement,
    run_bq_sql,
    create_bigquery_client,
)

from common.aws.aws_secret import get_secret

from .drupalapi.getarticles import (
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
    parse_content_meta,
)

logger("drupalapi")
load_dotenv()

API_PATH_ID = "display/content/"

drupal = get_secret("datateam_sentiment")
drupal_json = json.loads(drupal)

API_ENDPOINT = drupal_json["DRUPAL_CONTENT_API_PRD_NEW"]
API_USR = drupal_json["DRUPAL_CONTENT_API_PRD_USR"]
API_PW = drupal_json["DRUPAL_CONTENT_API_PRD_PW"]

google_cred_value = get_secret("datateam_google_cred_prod_base64")
google_cred_json = json.loads(google_cred_value)
google_cred = google_cred_json["GOOGLE_CLOUD_CRED_BASE64"]


def convert_to_dicts(content_metadata: List[ContentItemMeta]) -> List[dict]:
    """convert data into dictionary"""
    return [item.model_dump(by_alias=True) for item in content_metadata]


def process_content_items(  # pylint: disable=too-many-arguments
    session,
    endpoint,
    path_id,
    user,
    pwd,
    publication_channels: List[str],
    article_ids: List[str],
):
    """
    Get metadata for each content ID and publication channel combination.
    """

    all_content_metadata = []

    logging.info("Processing content items")

    for item_id in article_ids:
        for main_pub_key in publication_channels:
            try:
                if not main_pub_key:
                    logging.warning(
                        "No mainPublicationChannel key provided for item ID: %s",
                        item_id,
                    )
                    continue

                result_id = get_content_metadata(
                    session,
                    endpoint,
                    path_id,
                    item_id,
                    main_pub_key,
                    user,
                    pwd,
                )

                if result_id.status_code == 200:
                    content_meta = parse_content_meta(result_id.json())
                    logging.info(
                        "Fetched metadata for content ID %s with channel %s",
                        item_id,
                        main_pub_key,
                    )
                    all_content_metadata.append(content_meta)
                    # Stop checking other publication channels for this item_id
                    break

                logging.info(
                    "Failed to fetch metadata for content ID %s with channel %s: %s",
                    item_id,
                    main_pub_key,
                    result_id.status_code,
                )
            except Exception as errors:  # pylint: disable=broad-except

                logging.info(
                    "Exception occurred while processing item ID %s in process_content_items(): %s",
                    item_id,
                    errors,
                )

    return all_content_metadata


def drupal_run(publication_channels: List[str], article_ids: List[str]):
    """Main flow to process content based on article IDs and publication channels."""

    # ********************** #
    # overwrite this with the correct value for prod"
    # API_ENDPOINT = drupal_json["DRUPAL_CONTENT_API_PRD_NEW"]
    # use the following format:
    # API_ENDPOINT = "{url}/"
    # ********************** #

    dataset_id = drupal_json["DATASET_ID"]
    table_id = drupal_json["DRUPAL_TABLE_NAME"]
    temp_table_id = drupal_json["TEMP_DRUPAL_TABLE_NAME"]

    session = init_session()
    if session is None:
        logging.error("Failed to initialize session")
        sys.exit(1)
    logging.info("Session initialized")

    all_content_metadata = process_content_items(
        session,
        API_ENDPOINT,
        API_PATH_ID,
        API_USR,
        API_PW,
        publication_channels,
        article_ids,
    )

    if all_content_metadata:
        content_data_dicts = convert_to_dicts(all_content_metadata)
        df = pd.DataFrame(content_data_dicts)
        # Deduplication
        df = df.drop_duplicates(subset=["id"])

        client = create_bigquery_client(google_cred)

        create_table_if_not_exists(
            client, dataset_id, temp_table_id, BQ_SCHEMA
        )
        create_table_if_not_exists(client, dataset_id, table_id, BQ_SCHEMA)

        load_data_to_bigquery(client, df, dataset_id, temp_table_id, BQ_SCHEMA)

        primary_keys = ["id"]
        target_dataset_table = dataset_id + "." + table_id
        temp_dataset_table = dataset_id + "." + temp_table_id

        logging.info("temp_dataset_table: %s", temp_dataset_table)

        merge_sql = generate_bq_merge_statement(
            target_dataset_table, temp_dataset_table, BQ_SCHEMA, primary_keys
        )

        run_bq_sql(client, merge_sql)

    session.close()


if __name__ == "__main__":

    publication_channels_list = [
        "hexa",
        "the-post",
        "waikato-times",
        "the-press",
        "sunday-star-times",
        "the-timaru-herald",
        "taranaki-daily-news",
        "the-southland-times",
        "marlborough-express",
        "manawatu-standard",
        "nelson-mail",
    ]

    article_ids_list = [
        "1"
        # "350405574",
        # "350280176",
    ]

    drupal_run(publication_channels_list, article_ids_list)
