"""
functions for request/retrieve piano reports
"""

import time
import logging
from io import BytesIO
from zipfile import ZipFile
import requests
import pandas as pd


def request_vx_conv_report(url, app_id, app_api_token, start_date, end_date):
    """
    request vx conv report
    """

    try:
        params = {
            "aid": app_id,
            "api_token": app_api_token,
            "row_limit": 0,  # not limited if set to 0
            "from": start_date,
            "to": end_date,
        }

        # ?aid={app_id}&api_token={app_api_token}&row_limit=500'
        url = url + f"?aid={app_id}&api_token={app_api_token}&row_limit=0"
        # Make the POST request
        response = requests.post(url, params=params, timeout=10)

        # Check the response
        if response.status_code == 200:
            logging.info(
                "Report request successful in request_vx_conv_report() for %s - %s",
                start_date,
                end_date,
            )
            return response.json()
        logging.info("Failed to request report: HTTP %s", response.status_code)
        return response.text
    except Exception as error:  # pylint: disable=broad-except
        logging.info("error in request_vx_conv_report(): %s", error)
        return None


def request_composer_conv_report(
    url, app_id, app_api_token, exp_id, start_date, end_date
):  # pylint: disable=too-many-arguments,too-many-locals
    """
    Request a Composer Conversion Report for a single experience ID.

    Parameters:
    - url: API endpoint URL for requesting the Composer Conversion Report.
    - app_id: The application ID.
    - app_api_token: The API token for authentication.
    - exp_id: The experience ID for which the report is requested.
    - start_date: The report start date in the application time zone (yyyy-MM-dd).
    - end_date: The report end date in the application time zone (yyyy-MM-dd).
    - file_name: Optional. The name of the generated file without an extension.

    Returns:
    - The JSON response containing the report request info or error details.
    """
    try:
        params = {
            "aid": app_id,
            "api_token": app_api_token,
            "exp_id": exp_id,
            "row_limit": 0,  # Not limited if set to 0
            "from": start_date,
            "to": end_date
            # 'file_name': file_name if file_name else exp_id
        }

        # Make the POST request
        # Use json=params to ensure data is sent as JSON
        response = requests.post(url, params=params, timeout=10)

        # Check the response
        if response.status_code == 200:
            logging.info(
                "Report request successful for Experience ID %s from %s to %s",
                exp_id,
                start_date,
                end_date,
            )
            return response.json()
        logging.error(
            "Failed to request report for Experience ID %s: HTTP %s",
            exp_id,
            response.status_code,
        )
        return {
            "error": "Failed to request report",
            "status_code": response.status_code,
            "exp_id": exp_id,
        }

    except Exception as error:  # pylint: disable=broad-except
        logging.error("Error in request_composer_conv_report(): %s", error)
        return {"error": str(error)}


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
    Download a report ZIP file from Piano using the export ID and load its CSV contents
    into separate pandas DataFrames without saving the ZIP file to disk.

    Parameters:
    - app_id: The application ID.
    - app_api_token: The API token for authentication.
    - export_id: The export ID of the report to download.

    Returns:
    - A dictionary of DataFrames, with keys corresponding to the filenames of the CSV files.
    """
    # Endpoint to get the download URL
    get_url_endpoint = "https://reports-api.piano.io/rest/export/download/url"

    # Parameters for the GET request to obtain the download URL
    params = {
        "aid": app_id,
        "export_id": export_id,
        "api_token": app_api_token,
    }

    # Make the GET request to obtain the download URL
    response = requests.get(get_url_endpoint, params=params, timeout=10)

    dataframes = {}  # Initialize an empty dictionary to store DataFrames

    if response.status_code == 200:
        download_url = response.json().get("url")
        if download_url:
            # Download the ZIP file content
            file_response = requests.get(download_url, timeout=10)
            if file_response.status_code == 200:
                zip_in_memory = BytesIO(file_response.content)
                with ZipFile(zip_in_memory, "r") as myzip:
                    for zip_info in myzip.infolist():
                        if zip_info.filename.endswith(".csv"):
                            with myzip.open(zip_info) as file_in_zip:
                                df = pd.read_csv(file_in_zip)
                                # Add the DataFrame to the dictionary
                                dataframes[zip_info.filename] = df
                return dataframes
            logging.info(
                "Failed to download the report: HTTP %s",
                file_response.status_code,
            )
        logging.info("Download URL not found in the response.")
    else:
        logging.info(
            "Failed to obtain download URL: HTTP %s", response.status_code
        )
    return dataframes
