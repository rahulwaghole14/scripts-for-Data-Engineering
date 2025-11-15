"""
test
"""
from datetime import datetime, timedelta
import os
import logging

from dotenv import load_dotenv

from a_common.bigquery.bigquery import create_bigquery_client
from .extract_dates import extract_date_from_log
from .extract_data import extract_data_for_regional
from .extract_data import extract_data_for_mobile_device

# from .common.paths import SECRET_PATH
from .logging_module import logger

load_dotenv()
logger()

google_cred = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")

if __name__ == "__main__":

    path = (
        os.getcwd()
        + "\\src\\adobe_analytics__sponsored_content_to_bigquery\\log\\"
    )
    # logging.info(path)
    # wshould read from BQ table instead of log
    start_date, end_date = extract_date_from_log(path)
    tables = ["mobile_device", "regional"]

    # getting missing start_dates if any
    miss_tbl = list(set(tables) - set(list(start_date.keys())))

    # bigquery parameters and credentials
    PROJECT_ID = "hexa-data-report-etl-prod"
    DATASET_ID = "sponsored_content"
    client = create_bigquery_client(google_cred)

    if len(miss_tbl) > 0:
        logging.info("Getting missing start dates from BQ")
        for tbl in miss_tbl:

            query = (
                "SELECT max(Date) \
                    FROM \
                    `"
                + PROJECT_ID
                + "."
                + DATASET_ID
                + "."
                + tbl
                + "`"
            )
            data = client.query(query).result().to_dataframe()
            start_date[tbl] = data["f0_"].iloc[0] + timedelta(days=1)
            end_date[tbl] = datetime.now().date()

    # logging.info(start_date)
    # logging.info(end_date)

    if bool(start_date):
        # mobile device
        if (end_date["mobile_device"] - start_date["mobile_device"]).days > 0:
            extract_data_for_mobile_device(
                PROJECT_ID + "." + DATASET_ID + ".mobile_device",
                client,
                start_date["mobile_device"],
                end_date["mobile_device"],
            )
        else:
            logging.info("Mobile device is up to date!")
        # regional
        if (end_date["regional"] - start_date["regional"]).days > 0:
            extract_data_for_regional(
                PROJECT_ID + "." + DATASET_ID + ".regional",
                client,
                start_date["regional"],
                end_date["regional"],
            )
        else:
            logging.info("Regional is up to date!")
    else:
        logging.info("Empty start date dict!")

    # '''
    # # for manual update
    # extract_data_for_mobile_device(PROJECT_ID+'.'+DATASET_ID+'.mobile_device',
    #                                 client,
    #                                 date(2022, 5, 2),
    #                                 date(2022, 5, 3))

    # extract_data_for_regional(PROJECT_ID+'.'+DATASET_ID+'.regional',
    #                                 client,
    #                                 date(2022, 3, 30),
    #                                 date(2022, 3, 31))
    # '''
