"""
get local files piano into bigquery
"""
# pylint: disable=line-too-long

import os
import logging
import sys
import json
import re
from datetime import datetime, timedelta
import pandas as pd
import pytz

from common.bigquery.bigquery import (
    create_bigquery_client,
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    load_data_to_bigquery,
)
from common.validation.validators import validate_dataframe
from common.logging.logger import logger
from common.aws.aws_secret import get_secret


from .piano_reports import (
    request_vx_conv_report,
    check_report_status,
    download_report_and_load_csvs_to_dataframes,
    request_composer_conv_report,
)

from .validator import get_model_from_filename

logger(
    "piano_csvs",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


def initialize_config():
    """setup config"""
    piano_secret = get_secret("datateam_piano_cred")
    data = json.loads(piano_secret)
    # PIANO_APP_IDS = data["PIANO_APP_IDS"]
    # PIANO_APP_TOKENS = data["PIANO_APP_TOKENS"]
    # PIANO_APP_NAMES = data["PIANO_APP_NAMES"]
    # PIANO_APP_EXPERIENCE_IDS = data["PIANO_APP_EXPERIENCE_IDS"]
    piano_app_ids = data["PIANO_APP_IDS"]
    piano_app_tokens = data["PIANO_APP_TOKENS"]
    piano_app_names = data["PIANO_APP_NAMES"]
    piano_app_experience_ids = data["PIANO_APP_EXPERIENCE_IDS"]

    google_cred_secret = get_secret("datateam_google_cred_prod_base64")
    data_google = json.loads(google_cred_secret)

    google_cred = data_google["GOOGLE_CLOUD_CRED_BASE64"]

    local_config_dict = {
        "project_id": "hexa-data-report-etl-prod",
        "dataset_id": "cdw_stage",
        "datasource": "piano",
        "piano_app_id": "none",
        "piano_app_api_token": "none",
        "piano_app_experience_id": "none",
        "piano_report_vx_conversion_endpoint": "https://reports-api.piano.io/"
        "rest/export/schedule/vx/conversion",
        "piano_report_composer_endpoint": "https://reports-api.piano.io/rest/"
        "export/schedule/composer/conversion",
        "piano_report_status_endpoint": "https://reports-api.piano.io/rest/export/status",
        "piano_app_ids": piano_app_ids,
        "piano_app_api_tokens": piano_app_tokens,
        "piano_app_names": piano_app_names,
        "piano_app_experience_ids": piano_app_experience_ids,
        "google_cred_base64": google_cred,
    }

    return local_config_dict


def process_report(
    datasetid,
    source,
    report_type,
    appid,
    appname,
    dataframes,
    bqclient,
    daystart,
):  # pylint: disable=too-many-arguments,too-many-locals
    """process report for piano"""
    for file, dataframe in dataframes.items():
        try:
            dataframe["report_type"] = report_type
            dataframe["app_id"] = appid
            dataframe["app_name"] = appname
            dataframe["date_at"] = daystart
            dataframe["date_at"] = pd.to_datetime(dataframe["date_at"])

            dataframe.columns = [
                re.sub(r"[^a-zA-Z0-9]", "_", col) for col in dataframe.columns
            ]

            basemodel, basename = get_model_from_filename(file)

            validateddata = validate_dataframe(dataframe, basemodel)

            bqschema = pydantic_model_to_bq_schema(basemodel)
            dims = ["date_at", "app_id"]
            tableid = f"{source}__{report_type}_{basename}".lower()

            create_table_if_not_exists(bqclient, datasetid, tableid, bqschema)
            load_data_to_bigquery(
                bqclient, datasetid, tableid, validateddata, dims, bqschema
            )

        except ValueError as evalue:
            logging.error("Validation model missing or data issue: %s", evalue)
        except Exception as eexcpt:  # pylint: disable=broad-except
            logging.error(
                "Unexpected error during validation of %s: %s", file, eexcpt
            )


def conversions():

    logging.info("== START SCRIPT ==")
    local_config = initialize_config()
    logging.info("-- config loaded --")

    # today = datetime.today()
    # Define the NZT timezone
    nztime = pytz.timezone("Pacific/Auckland")
    # Get the current time in NZT
    nz_now = datetime.now(nztime)
    today = nz_now.replace(tzinfo=None)

    # start_date = datetime(2024, 2, 23) # backfill
    # end_date = datetime(2024, 2, 26) # backfill
    start_date = today - timedelta(days=4)
    end_date = today - timedelta(days=1)
    logging.info(
        "date range set to %s - %s",
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    current_date = start_date
    while current_date <= end_date:
        day_start = current_date.strftime("%Y-%m-%d")
        day_end = current_date.strftime("%Y-%m-%d")
        status_endpoint = local_config["piano_report_status_endpoint"]
        endpoint = local_config["piano_report_vx_conversion_endpoint"]

        logging.info("-- Processing for date: %s --", day_start)
        logging.info("-- get vxconversionreport --")

        for app_id, app_token, app_name in zip(
            local_config["piano_app_ids"],
            local_config["piano_app_api_tokens"],
            local_config["piano_app_names"],
        ):

            logging.info("Processing vxconversionreport for %s", app_name)

            # app_id = local_config['piano_app_id']
            # app_token = local_config['piano_app_api_token']
            endpoint = local_config["piano_report_vx_conversion_endpoint"]

            data = request_vx_conv_report(
                endpoint, app_id, app_token, day_start, day_end
            )

            export_id = data.get("export_id")

            result = check_report_status(
                status_endpoint, app_id, app_token, export_id
            )

            if result:
                job_status = result.get("job_status")
                if job_status == "FINISHED":
                    logging.info(
                        "Report generation finished. Proceeding to download."
                    )
                elif job_status in ["FAILED", "INTERNAL_ERROR"]:
                    logging.error(
                        "Report generation failed with status: %s. Exiting.",
                        job_status,
                    )
                    sys.exit(1)

                dfs = download_report_and_load_csvs_to_dataframes(
                    app_id, app_token, export_id
                )

                client = create_bigquery_client(
                    local_config["google_cred_base64"]
                )

                process_report(
                    local_config["dataset_id"],
                    local_config["datasource"],
                    "vxConversionReport",
                    app_id,
                    app_name,
                    dfs,
                    client,
                    day_start,
                )

        logging.info("-- get composerreport --")

        for app_id, app_token, app_name, app_exp_id in zip(
            local_config["piano_app_ids"],
            local_config["piano_app_api_tokens"],
            local_config["piano_app_names"],
            local_config["piano_app_experience_ids"],
        ):

            logging.info("Processing composerreport for %s", app_name)

            endpoint_composer = local_config["piano_report_composer_endpoint"]

            logging.info(
                "Requesting report for App ID: %s, App Name: %s, Experience ID: %s",
                app_id,
                app_name,
                app_exp_id,
            )
            data_composer = request_composer_conv_report(
                endpoint_composer,
                app_id,
                app_token,
                app_exp_id,
                day_start,
                day_end,
            )

            export_id_composer = data_composer.get("export_id")

            result_composer = check_report_status(
                status_endpoint, app_id, app_token, export_id_composer
            )

            if result_composer:
                job_status_composer = result_composer.get("job_status")
                if job_status_composer == "FINISHED":
                    logging.info(
                        "Report generation finished. Proceeding to download."
                    )
                elif job_status_composer in ["FAILED", "INTERNAL_ERROR"]:
                    logging.error(
                        "Report generation failed with status: %s. Exiting.",
                        job_status_composer,
                    )
                    sys.exit(1)

                dfs_composer = download_report_and_load_csvs_to_dataframes(
                    app_id, app_token, export_id_composer
                )

                client = create_bigquery_client(
                    local_config["google_cred_base64"]
                )

                process_report(
                    local_config["dataset_id"],
                    local_config["datasource"],
                    "ComposerReport",
                    app_id,
                    app_name,
                    dfs_composer,
                    client,
                    day_start,
                )

        current_date += timedelta(days=1)

    logging.info("== END SCRIPT ==")
