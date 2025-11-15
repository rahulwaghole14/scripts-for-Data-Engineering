"""
get local files piano into bigquery
"""
# pylint: disable=line-too-long,inconsistent-return-statements

import os
import logging
import json
import time
import pytz
from datetime import datetime, timedelta
from functools import wraps

# from common.validation.validators import validate_dataframe
from common.logging.logger import logger
from common.bigquery.bigquery import create_bigquery_client
from common.aws.aws_secret import get_secret

from .piano_reports import (
    request_vx_subscription_log_report,
    check_report_status,
    download_report_and_load_csvs_to_dataframes,
    process_report,
)

# from .validator import get_model_from_filename

logger(
    "piano_csvs",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


def initialize_config():
    """setup config"""
    piano_secret = get_secret("datateam_piano_cred")
    data = json.loads(piano_secret)
    piano_app_ids = data["PIANO_APP_IDS"]
    piano_app_tokens = data["PIANO_APP_TOKENS"]
    piano_app_names = data["PIANO_APP_NAMES"]
    google_cred_secret = get_secret("datateam_google_cred_prod_base64")
    data_google = json.loads(google_cred_secret)
    google_cred = data_google["GOOGLE_CLOUD_CRED_BASE64"]

    local_config_dict = {
        "project_id": "hexa-data-report-etl-prod",
        "dataset_id": "cdw_stage",
        "datasource": "piano",
        "piano_report_vx_subscription_log": "https://reports-api.piano.io/rest/export/schedule/vx/subscriptionLog",
        "piano_report_status_endpoint": "https://reports-api.piano.io/rest/export/status",
        "piano_app_ids": piano_app_ids,
        "piano_app_api_tokens": piano_app_tokens,
        "piano_app_names": piano_app_names,
        "google_cred_base64": google_cred,
    }

    return local_config_dict


class UnexpectedJobStatusError(Exception):
    """Exception raised when the job status is unexpected."""


def retry(
    max_attempts=3,
    delay=5,
    backoff=2,
    allowed_statuses=None,
    retry_on_exceptions=(RuntimeError,),
):
    """
    Decorator to retry a function if certain conditions are met.

    Args:
        max_attempts (int): Maximum number of attempts.
        delay (int): Initial delay between attempts in seconds.
        backoff (int): Multiplier for delay.
        allowed_statuses (list): List of statuses that should trigger a retry.
        retry_on_exceptions (tuple): Exception classes that should trigger a retry.

    Returns:
        Wrapped function with retry logic.
    """
    if allowed_statuses is None:
        allowed_statuses = ["FAILED", "INTERNAL_ERROR"]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    job_status = result.get("job_status")
                    if job_status == "FINISHED":
                        logging.info("Report generation finished.")
                        return result

                    if job_status in allowed_statuses:
                        logging.warning(
                            "Attempt %d/%d: Report generation %s.",
                            attempt,
                            max_attempts,
                            job_status,
                        )
                        if attempt < max_attempts:
                            logging.info(
                                "Retrying in %d seconds...", current_delay
                            )
                            time.sleep(current_delay)
                            current_delay *= backoff
                        else:
                            logging.error(
                                "Max retry attempts reached. Report generation %s.",
                                job_status,
                            )
                            raise RuntimeError(
                                f"Report generation failed with status: {job_status}"
                            )
                    else:
                        logging.error(
                            "Unexpected job status: %s. Exiting.", job_status
                        )
                        raise UnexpectedJobStatusError(
                            f"Unexpected job status: {job_status}"
                        )
                except retry_on_exceptions as e:
                    logging.error(
                        "An error occurred during report generation: %s",
                        str(e),
                    )
                    if attempt < max_attempts:
                        logging.info(
                            "Retrying in %d seconds...", current_delay
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logging.error("Max retry attempts reached. Exiting.")
                        raise

        return wrapper

    return decorator


@retry(
    max_attempts=3,
    delay=5,
    backoff=2,
    allowed_statuses=["FAILED", "INTERNAL_ERROR"],
)
def generate_and_check_report(
    status_endpoint, request_endpoint, app_id, app_token, start_date, end_date
):  # pylint: disable=too-many-arguments
    """
    Request report generation and check its status.

    Args:
        status_endpoint (str): The endpoint to check report status.
        request_endpoint (str): The endpoint to request report generation.
        app_id (str): Application ID.
        app_token (str): Application token.
        start_date (str): Start date for the report.
        end_date (str): End date for the report.

    Returns:
        dict: Result from check_report_status.
    """
    data = request_vx_subscription_log_report(
        request_endpoint,
        app_id,
        app_token,
        start_date,
        end_date,
    )

    export_id = data.get("export_id")

    if not export_id:
        logging.error("No export_id returned from report request.")
        raise ValueError("Failed to obtain export_id.")

    result = check_report_status(status_endpoint, app_id, app_token, export_id)

    if not result:
        logging.error("No result returned from check_report_status.")
        raise ValueError("Failed to check report status.")

    return result


def main():
    """
    Main function to generate, download, and process vxsubscriptionlog reports.
    """

    logging.info("== START SCRIPT ==")
    local_config = initialize_config()
    logging.info("-- config loaded --")
    nztime = pytz.timezone("Pacific/Auckland")
    nz_now = datetime.now(nztime)

    # today = datetime.today()
    today = nz_now.replace(tzinfo=None)
    # we get the last one week's report to load to bigquery.
    start_date = start_date = datetime(2023, 1, 1)
    end_date = today - timedelta(days=1)
    logging.info(
        "Date range set to %s - %s",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    status_endpoint = local_config["piano_report_status_endpoint"]
    endpoint = local_config["piano_report_vx_subscription_log"]

    for app_id, app_token, app_name in zip(
        local_config["piano_app_ids"],
        local_config["piano_app_api_tokens"],
        local_config["piano_app_names"],
    ):

        logging.info("Processing vxsubscriptionlog for %s", app_name)

        try:
            # Generate report and check status with retry logic
            result = generate_and_check_report(
                status_endpoint,
                endpoint,
                app_id,
                app_token,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                "Failed to generate report for %s after retries: %s",
                app_name,
                str(e),
            )
            continue  # Skip to the next app

        job_status = result.get("job_status")
        if job_status == "FINISHED":
            logging.info(
                "Report generation finished for %s. Proceeding to download.",
                app_name,
            )

        # Proceed to download and process the report
        try:
            dfs = download_report_and_load_csvs_to_dataframes(
                app_id, app_token, result.get("export_id")
            )

            client = create_bigquery_client(local_config["google_cred_base64"])

            process_report(
                local_config["dataset_id"],
                local_config["datasource"],
                "vxsubscriptionlog",
                app_id,
                app_name,
                dfs,
                client,
            )
            logging.info("Successfully processed report for %s.", app_name)
        except Exception as e:  # pylint: disable=broad-except
            logging.error(
                "Error processing report for %s: %s",
                app_name,
                str(e),
            )

    logging.info("== END SCRIPT ==")


if __name__ == "__main__":
    main()
