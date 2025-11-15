"""
get missing data

"""

import logging

import os
from datetime import date

# from datetime import datetime, timedelta, date

# from extract_dates import extract_date_from_log
# from extract_data_fix import extract_data_for_regional
from extract_data_fix import extract_data_for_mobile_device

# big query
from google.cloud import bigquery

# from google.cloud.exceptions import NotFound
# from google.cloud import bigquery_storage #_v1beta1
from google.oauth2 import service_account
from logging_module import logger
from common.paths import SECRET_PATH


logger("missing_data")


if __name__ == "__main__":

    try:
        day = 1
        month = 10
        year = 2023

        mobile_device_date = date(year, month, day)
        regional_date = date(year, month, day)

        logging.info("START MAIN SCRIPT")

        tables = ["mobile_device", "regional"]
        # getting missing start_dates if any
        start_date = {
            "mobile_device": mobile_device_date,
            "regional": regional_date,  # Example date, adjust as needed
        }

        miss_tbl = list(set(tables) - set(list(start_date.keys())))
        logging.info("Current Working Directory: %s", os.getcwd())

        # bigquery parameters and credentials
        project_id = "hexa-data-report-etl-prod"
        dataset_id = "sponsored_content"
        # key_path = 'src/adobe_analytics__sponsored_content_to_bigquery/secrets/hexa-data-report-etl-prod-5b93b81b644e.json' for local testing
        key_path = "./secrets/hexa-data-report-etl-prod-5b93b81b644e.json"

        logging.info("key_path")
        logging.info(key_path)
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

        # for manual update with the missing data date range
        extract_data_for_mobile_device(
            project_id + "." + dataset_id + ".mobile_device",
            client,
            date(2023, 10, 1),  # start date
            date(2024, 1, 14),
        )  # end date

        # # Currently regional data is not used in the Sponsored content dashboard
        # extract_data_for_regional(project_id+'.'+dataset_id+'.regional',
        #                                 client,
        #                                 date(2022, 6, 1), # start date
        #                                 date(2022, 7, 27)) # end date
    except Exception as error:
        logging.info("Error in missing_data script %s", str(error))
