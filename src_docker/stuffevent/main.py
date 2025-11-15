"""
webflow workflow for hexaevent rtb and cdf
round the bays
central district fielddays
"""

import os
import logging
import json
from datetime import datetime, date, time
import requests

from common.logging.logger import logger
from common.aws.aws_secret import get_secret
from common.http.http import init_session
from common.validation.validators import validate_list_of_dicts_serialised
from common.bigquery.bigquery import (
    create_bigquery_client,
    pydantic_model_to_bq_schema,
    create_table_if_not_exists,
    load_data_to_bigquery_truncate,
    merge_temp_table_to_target,
)

from .validator import FormSubmission

# Configure logging for urllib3 to capture retry attempts
logging.getLogger("urllib3").setLevel(logging.INFO)
urllib3_logger = logging.getLogger("urllib3")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
urllib3_logger.addHandler(handler)


def get_site_id(session, webflow_key, site_name):
    """
    Get the site id for the rtb or cdf webflow site.
    """

    try:
        logging.info("Getting site id for %s", site_name)
        site_id_data = session.get(
            "https://api.webflow.com/v2/sites",
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {webflow_key}",
            },
            timeout=10,
        )
        site_id_data.raise_for_status()
        # logging.info("Site id data: %s", site_id_data.json())
        # logging.info("Site id: %s", site_id_data.json()["sites"][0]["id"])
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Failed to get site id: %s", e)
        return None

    return site_id_data.json()["sites"][0]["id"]


def get_form_ids(session, webflow_key, site_id, site_name, form_id_init=None):
    """
    Get the form ids for the site using pagination.
    """

    # Initialize query params
    limit = 25
    offset = 0

    all_form_ids = []

    if form_id_init is not None:
        all_form_ids = [
            form_id_init
        ]  # default form id workaround for missing form ids in webflow endpoint

    logging.info("Getting form ids for site %s", site_name)
    while True:
        try:
            # logging.info("Getting form ids for site %s with offset %s", site_name, offset)
            form_ids_data = session.get(
                f"https://api.webflow.com/v2/sites/{site_id}/forms?limit={limit}&offset={offset}",
                headers={
                    "accept": "application/json",
                    "authorization": f"Bearer {webflow_key}",
                },
                timeout=10,
            )
            form_ids_data.raise_for_status()
            forms_json = form_ids_data.json()

            # Extract form ids from the response
            forms = forms_json.get("forms", [])

            if not forms:
                break

            # Add form ids to the aggregate list
            all_form_ids.extend([form["id"] for form in forms])

            # Update the offset for the next page of data
            offset += limit

        except requests.exceptions.RequestException as e:
            logging.error(
                "Failed to get form ids for site %s: %s", site_name, e
            )
            return None

    return all_form_ids


def get_form_submissions(session, webflow_key, form_id):
    """
    Get all form submissions for the site and return a list of submissions.
    """

    # Initialize query params
    limit = 25
    offset = 0
    all_form_submissions = []

    while True:
        try:
            # logging.info("Getting form submissions for site %s with offset %s", site_name, offset)
            form_submissions_data = session.get(
                f"https://api.webflow.com/v2/forms/{form_id}"
                f"/submissions?limit={limit}&offset={offset}",
                headers={
                    "accept": "application/json",
                    "authorization": f"Bearer {webflow_key}",
                },
                timeout=10,
            )
            form_submissions_data.raise_for_status()
            submissions_json = form_submissions_data.json()

            # Extract form submissions from the response
            form_submissions = submissions_json.get("formSubmissions", [])

            # form_submissions_len = len(form_submissions)
            # logging.info("Form submissions for form %s: %s", form_id, form_submissions_len)

            if not form_submissions:
                break

            # Add form submissions to the aggregate list
            all_form_submissions.extend(form_submissions)

            # Update the offset for the next page of data
            offset += limit

        except requests.exceptions.RequestException as e:
            logging.info(
                "Failed to get form submissions for form %s: %s", form_id, e
            )
            return []

    return all_form_submissions


def get_all_forms_data(session, webflow_key, form_ids):
    """
    Fetch all form submissions across multiple forms and return a master list of dictionaries
    with form id and form response.
    """

    master_list = []

    logging.info(
        "Start fetching data from all forms, count of forms to fetch: %s",
        len(form_ids),
    )

    for form_id in form_ids:
        # logging.info("Fetching form submissions for form %s", form_id)
        form_submissions = get_form_submissions(session, webflow_key, form_id)
        # logging.info(
        #     "Form submissions for form %s: %s", form_id, len(form_submissions)
        # )
        # Append each submission to the master list with the form id included
        for submission in form_submissions:
            master_list.append(
                {
                    "id": submission.get("id", ""),
                    "displayName": submission.get("displayName", ""),
                    "siteId": submission.get("siteId", ""),
                    "formId": submission.get("formId", ""),
                    "formResponse": submission.get("formResponse", {}),
                    "dateSubmitted": submission.get("dateSubmitted", ""),
                }
            )

    return master_list


def main():
    """main flow"""

    logger(
        "hexaevent-workflow",
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
    )

    start_marker = "=" * 10
    logging.info("%s SCRIPT STARTED %s", start_marker, start_marker)

    # Initialize a session object
    session = init_session()

    # for each site in webflow we need a new key in the .env file
    # the keys can be found in the webflow dashboard (apps & integrations)

    hexaevent_webflow_json = json.loads(
        get_secret("datateam_hexaevent_webflow")
    )
    hexaevent_env = hexaevent_webflow_json["env"]
    logging.info("secrets location env: %s", hexaevent_env)

    # RTB ===========================================

    hexaevent_rtb = hexaevent_webflow_json["rtb_token"]
    form_id_init = hexaevent_webflow_json["rtb_form_id_init"]

    # fetch ids for the site and forms
    site_id_rtb = get_site_id(session, hexaevent_rtb, "hexaevent_rtb")
    form_ids_rtb = get_form_ids(
        session, hexaevent_rtb, site_id_rtb, "hexaevent_rtb", form_id_init
    )

    # logging.info("form ids for hexaevent_rtb: %s", form_ids_rtb)
    # Fetch all form data from multiple forms and validate
    logging.info("Fetching form submission data for hexaevent_rtb")
    form_submission_rtb_data = get_all_forms_data(
        session, hexaevent_rtb, form_ids_rtb
    )

    logging.info("Validating form submission data for hexaevent_rtb")
    form_submission_rtb_data = validate_list_of_dicts_serialised(
        form_submission_rtb_data, FormSubmission
    )

    # Deduplication of form submissions based on composite primary key: id, siteId, formId
    form_submission_rtb_data = list(
        {
            (form["id"], form["siteId"], form["formId"]): form
            for form in form_submission_rtb_data
        }.values()
    )

    # print length of the collected data
    logging.info(
        "Total form submissions collected: %d, writing to bigquery...",
        len(form_submission_rtb_data),
    )

    # Write the data to bigquery

    google_cred = json.loads(get_secret("datateam_google_cred_prod_base64"))[
        "GOOGLE_CLOUD_CRED_BASE64"
    ]
    client = create_bigquery_client(google_cred)
    type_mapping = {
        int: "INTEGER",
        str: "STRING",
        float: "FLOAT",
        bool: "BOOLEAN",
        datetime: "TIMESTAMP",
        date: "DATE",
        time: "TIME",
        dict: "JSON",
    }
    hexaevent_form_schema = pydantic_model_to_bq_schema(
        FormSubmission, type_mapping
    )
    google_dataset = "cdw_stage_hexaevent"
    google_table_id = "hexaevent_formsubmissions"
    temp_table_id = "temp_hexaevent_formsubmissions"
    primary_keys = ["id", "siteId", "formId"]

    create_table_if_not_exists(
        client,
        google_dataset,
        google_table_id,
        hexaevent_form_schema,
        "dateSubmitted",
    )

    create_table_if_not_exists(
        client,
        google_dataset,
        temp_table_id,
        hexaevent_form_schema,
        "dateSubmitted",
    )

    load_data_to_bigquery_truncate(
        client,
        google_dataset,
        temp_table_id,
        form_submission_rtb_data,
        hexaevent_form_schema,
    )

    merge_temp_table_to_target(
        client,
        f"{google_dataset}.{google_table_id}",
        f"{google_dataset}.{temp_table_id}",
        hexaevent_form_schema,
        primary_keys,
    )

    # CDF ===========================================

    hexaevent_cdf = hexaevent_webflow_json["cdf_token"]
    form_id_init_cdf = hexaevent_webflow_json["cdf_form_id_init"]

    # fetch ids for the site and forms
    site_id_cdf = get_site_id(session, hexaevent_cdf, "hexaevent_cdf")
    form_ids_cdf = get_form_ids(
        session,
        hexaevent_cdf,
        site_id_cdf,
        "hexaevent_cdf",
        form_id_init_cdf,
    )

    # logging.info("form ids for hexaevent_cdf: %s", form_ids_cdf)
    # Fetch all form data from multiple forms and validate
    logging.info("Fetching form submission data for hexaevent_cdf")
    form_submission_cdf_data = get_all_forms_data(
        session, hexaevent_cdf, form_ids_cdf
    )

    logging.info("Validating form submission data for hexaevent_cdf")
    form_submission_cdf_data = validate_list_of_dicts_serialised(
        form_submission_cdf_data, FormSubmission
    )

    # Deduplication of form submissions based on composite primary key: id, siteId, formId
    form_submission_cdf_data = list(
        {
            (form["id"], form["siteId"], form["formId"]): form
            for form in form_submission_cdf_data
        }.values()
    )

    # print length of the collected data
    logging.info(
        "Total form submissions collected: %d, writing to bigquery...",
        len(form_submission_cdf_data),
    )

    # Write the data to bigquery

    google_cred = json.loads(get_secret("datateam_google_cred_prod_base64"))[
        "GOOGLE_CLOUD_CRED_BASE64"
    ]

    client = create_bigquery_client(google_cred)

    hexaevent_form_schema_cdf = pydantic_model_to_bq_schema(
        FormSubmission, type_mapping
    )
    google_dataset_cdf = "cdw_stage_hexaevent"
    google_table_id_cdf = "hexaevent_formsubmissions"
    temp_table_id_cdf = "temp_hexaevent_formsubmissions"
    primary_keys = ["id", "siteId", "formId"]

    load_data_to_bigquery_truncate(
        client,
        google_dataset_cdf,
        temp_table_id_cdf,
        form_submission_cdf_data,
        hexaevent_form_schema_cdf,
    )

    merge_temp_table_to_target(
        client,
        f"{google_dataset_cdf}.{google_table_id_cdf}",
        f"{google_dataset_cdf}.{temp_table_id_cdf}",
        hexaevent_form_schema_cdf,
        primary_keys,
    )

    logging.info("%s SCRIPT ENDED SUCCESSFULLY %s", start_marker, start_marker)


if __name__ == "__main__":
    main()
