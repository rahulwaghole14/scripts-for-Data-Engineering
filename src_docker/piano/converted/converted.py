"""
converted terms to bigquery
"""

import os
import logging
import datetime
import json
import pytz
from google.cloud import bigquery
from common.logging.logger import logger
from common.bigquery.bigquery import (
    create_bigquery_client,
    load_data_to_bigquery_truncate,
)
from common.aws.aws_secret import get_secret
from .piano import request_send


logger(
    "piano", os.path.join(os.path.dirname(os.path.realpath(__file__)), "log")
)
piano_secret = get_secret("datateam_piano_cred")
data_piano = json.loads(piano_secret)

PIANO_ENDPOINT_CT = data_piano["PIANO_ENDPOINT_CT"]
PIANO_API_REQUEST_CT = data_piano["PIANO_API_REQUEST_CT"]
PIANO_APP_TOKENS = data_piano["PIANO_APP_TOKENS"]
google_cred_secret = get_secret("datateam_google_cred_prod_base64")
data_google = json.loads(google_cred_secret)

google_cred = data_google["GOOGLE_CLOUD_CRED_BASE64"]

LAST_ACCESS_TIME_FROM = "1682683200"  # 2023-01-01 00:00:00
BQ_DATASET = "cdw_stage"
BQ_TABLE = "piano__converted_terms"
schema = []
schema.append(bigquery.SchemaField("app_name", "STRING", mode="REQUIRED"))
schema.append(bigquery.SchemaField("total", "INTEGER", mode="REQUIRED"))
schema.append(bigquery.SchemaField("update_dts", "TIMESTAMP", mode="REQUIRED"))

# construct the request
url = PIANO_ENDPOINT_CT + PIANO_API_REQUEST_CT
url_thepress = url + (
    f"?converted_terms=TM3YDM4DVNV4&converted_terms=TMB7OOBQM0N9"
    f"&has_last_access_time=true&last_access_time_from={LAST_ACCESS_TIME_FROM}"
    f"&has_conversion_term=true&aid=go7g2STDpa"
)
url_waikatotimes = url + (
    f"?converted_terms=TM93EGSFZ3EW&converted_terms=TM2F1B22ZZTI"
    f"&has_last_access_time=true&last_access_time_from={LAST_ACCESS_TIME_FROM}"
    f"&has_conversion_term=true&aid=0V1Vwkflpa"
)
url_thepost = url + (
    f"?converted_terms=TMIQLH7C1OOH&converted_terms=TMSTPDPB15YF"
    f"&has_last_access_time=true&last_access_time_from={LAST_ACCESS_TIME_FROM}"
    f"&has_conversion_term=true&aid=tISrUfqypa"
)

LAST_ACCESS_TIME_FROM_NEW = "1713355200"  # 2024-04-18 00:00:00

url_thepost_new = url + (
    f"?converted_terms=TM3JARAI579Y&converted_terms=TMXEEM46L7UE"
    f"&converted_terms=TM0P49DGTT5Y&converted_terms=TMYE85DNN34U"
    f"&converted_terms=TM8AQFOGTLKN&converted_terms=TMCWWQCOPYSK"
    f"&has_last_access_time=true&last_access_time_from={LAST_ACCESS_TIME_FROM_NEW}"
    f"&has_conversion_term=true&aid=tISrUfqypa"
)


def converted():

    "Main function to run Piano converted terms tasks"

    try:
        logging.info("converted terms to bigquery")
        data_thepress = request_send(url_thepress, PIANO_APP_TOKENS[0])
        data_waikatotimes = request_send(url_waikatotimes, PIANO_APP_TOKENS[1])
        data_thepost = request_send(url_thepost, PIANO_APP_TOKENS[2])
        data_thepost_new = request_send(url_thepost_new, PIANO_APP_TOKENS[2])
        nztime = pytz.timezone("Pacific/Auckland")

        # merge data into one dict with update_dts column
        update_dts = datetime.datetime.now(nztime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        data = [
            {
                "app_name": "The Press",
                "total": data_thepress["total"],
                "update_dts": update_dts,
            },
            {
                "app_name": "Waikato Times",
                "total": data_waikatotimes["total"],
                "update_dts": update_dts,
            },
            {
                "app_name": "The Post",
                "total": data_thepost["total"] + data_thepost_new["total"],
                "update_dts": update_dts,
            },
        ]

        # logging.info('got data: %s', data)
        logging.info("got the data successfully")
        client = create_bigquery_client(google_cred)
        load_data_to_bigquery_truncate(
            client, BQ_DATASET, BQ_TABLE, data, schema
        )
    except Exception as error:  # pylint: disable=broad-except
        logging.info("error in main: %s", error)
        raise error
