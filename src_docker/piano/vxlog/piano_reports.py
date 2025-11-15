"""
functions for request/retrieve piano reports
"""

import time
import logging
import re
from io import BytesIO
import requests
import pandas as pd
from common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    run_bq_sql,
    generate_bq_merge_statement,
    load_data_to_bigquery_truncate,
)
from common.validation.validators import validate_dataframe
from .validation import VxSubscriptionLog


def request_vx_subscription_log_report(
    url, app_id, app_api_token, start_date, end_date
):
    """
    request_vx_subscription_log_report
    """

    try:

        url = (
            url
            + f"""?aid={app_id}&api_token={app_api_token}&start_date_from={start_date}&start_date_to={end_date}"""
        )
        # Make the POST request
        response = requests.post(url, timeout=20)

        # Check the response
        if response.status_code == 200:
            logging.info(
                "Report request successful in request_vx_subscription_log_report() for %s - %s",
                start_date,
                end_date,
            )
            return response.json()
        logging.info("Failed to request report: HTTP %s", response.status_code)
        return response.text
    except Exception as error:  # pylint: disable=broad-except
        logging.info(
            "error in request_vx_subscription_log_report(): %s", error
        )
        return None


def check_report_status(url, app_id, app_api_token, export_id):
    """
    Check the status of a report export job until it is finished.

    Parameters:
    - app_id: The application ID
    - app_api_token: The API token for authentication
    - export_id: The ID of the export job to check
    """

    # Interval in seconds between status checks
    check_interval = 10

    while True:
        try:
            # Parameters for the GET request
            params = {
                "aid": app_id,
                "api_token": app_api_token,
                "export_id": export_id,
            }

            # Make the GET request
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                job_status = data.get("job_status", "")

                logging.info("Current job status: %s", job_status)

                if job_status == "FINISHED":
                    # logging.info("Report generation finished successfully.")
                    return data
                if job_status in ["FAILED", "INTERNAL_ERROR"]:
                    logging.error(
                        "Report generation failed with status: %s", job_status
                    )
                    return data
                logging.info(
                    "Report generation in progress. Waiting before next check..."
                )
                time.sleep(check_interval)
            else:
                logging.error(
                    "Failed to check report status: HTTP %s",
                    response.status_code,
                )
                return None
        except Exception as error:  # pylint: disable=broad-except
            logging.error("Error checking report status: %s", error)
            return None


def download_report_and_load_csvs_to_dataframes(
    app_id, app_api_token, export_id
):
    """
    Download a csv file from Piano using the export ID and load its CSV contents
    into a pandas DataFrame without saving to disk.

    Parameters:
    - app_id: The application ID.
    - app_api_token: The API token for authentication.
    - export_id: The export ID of the report to download.

    Returns:
    - A dictionary of DataFrames, with keys corresponding to the filename of the CSV file.
    """
    # Endpoint to get the download URL
    get_url_endpoint = "https://reports-api.piano.io/rest/export/download/url"

    # Parameters for the GET request to obtain the download URL
    params = {
        "aid": app_id,
        "export_id": export_id,
        "api_token": app_api_token,
    }

    logging.info("Requesting download URL for app ID: %s", app_id)
    # Make the GET request to obtain the download URL
    response = requests.get(get_url_endpoint, params=params, timeout=10)

    # print response
    # logging.info(response.json())

    if response.status_code == 200:
        download_url = response.json().get("url")
        if download_url:
            logging.info("Download URL obtained for app_id: %s", app_id)
            # Download the file content
            file_response = requests.get(download_url, timeout=10)
            if file_response.status_code == 200:
                # open the csv file and load it into a pandas DataFrame
                with BytesIO(file_response.content) as file:
                    # return file content from csv into dictionary:
                    file = pd.read_csv(file)
                    return file

        # logging.info("Download URL not found in the response.")
    else:
        logging.info(
            "Failed to obtain download URL: HTTP %s", response.status_code
        )
    return None


def process_report(
    datasetid, source, report_type, appid, appname, dataframe, bqclient
):  # pylint: disable=too-many-arguments,too-many-locals
    """process report for piano"""
    try:
        dataframe["report_type"] = report_type
        dataframe["app_id"] = appid
        dataframe["app_name"] = appname
        dataframe["record_load_dts"] = pd.to_datetime("now")

        dataframe.columns = [
            re.sub(r"[^a-zA-Z0-9]", "_", col) for col in dataframe.columns
        ]

        validateddata = validate_dataframe(dataframe, VxSubscriptionLog)

        bqschema = pydantic_model_to_bq_schema(VxSubscriptionLog)
        primary_keys = ["Subscription_ID", "app_id"]
        tableid = f"{source}__{report_type}".lower()
        temp_tableid = f"{source}__temp_{report_type}".lower()

        # load data to bigquery merge from temp table
        create_table_if_not_exists(bqclient, datasetid, tableid, bqschema)
        create_table_if_not_exists(bqclient, datasetid, temp_tableid, bqschema)
        load_data_to_bigquery_truncate(
            bqclient, datasetid, temp_tableid, validateddata, bqschema
        )
        target_dataset_table = datasetid + "." + tableid
        temp_dataset_table = datasetid + "." + temp_tableid

        merge_sql = generate_bq_merge_statement(
            target_dataset_table, temp_dataset_table, bqschema, primary_keys
        )

        run_bq_sql(bqclient, merge_sql)

    except ValueError as evalue:
        logging.error("Validation model missing or data issue: %s", evalue)
    except Exception as eexcpt:  # pylint: disable=broad-except
        logging.error(
            "Unexpected error during validation of %s: %s", appname, eexcpt
        )
