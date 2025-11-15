"""
piano get subs functions
"""

import logging
import requests


def deduplicate_list_of_dicts(list_of_dicts, key):
    """deduplicate list of dicts by key"""
    seen = set()
    deduplicated = []
    for item in list_of_dicts:
        if item[key] not in seen:
            deduplicated.append(item)
            seen.add(item[key])
    return deduplicated


def fetch_all_data(
    endpont, apirequest, appid, apptoken, ts, record_limit
):  # pylint: disable=too-many-arguments,too-many-locals
    """fetch all data in pages from api"""

    try:
        # build url, set the app_id, app_name, and app_token for the current iteration
        url = endpont + apirequest
        url += "?aid=" + appid
        url += "&api_token=" + apptoken
        url += "&start_date=" + ts
        # comment out because we want all subs "updated" within last 48 hours

        offset = 0
        datalist = []
        total_records = None

        logging.info("capturing updated records")
        while total_records is None or offset < total_records:
            fetched_data, limit, new_offset, total = query_endpoint(
                url + f"&offset={offset}", record_limit
            )
            if fetched_data is None:
                break

            datalist.extend(fetched_data)
            offset = new_offset + limit
            total_records = total

        # capture created records
        url += "&select_by=create"
        offset = 0
        total_records = None

        logging.info("capturing created records select_by=create")
        while total_records is None or offset < total_records:
            fetched_data, limit, new_offset, total = query_endpoint(
                url + f"&offset={offset}", record_limit
            )
            if fetched_data is None:
                break

            datalist.extend(fetched_data)
            offset = new_offset + limit
            total_records = total

        # deduplicate data in datalist by subscription_id key before merging to temp table
        datalist = deduplicate_list_of_dicts(datalist, "subscription_id")

    except Exception as err:  # pylint: disable=broad-except
        logging.info("error in fetch_all_data() %s", err)

    return datalist


def query_endpoint(input_url, limit):
    """Query endpoint and return data or None in case of error."""

    input_url += "&limit=" + str(limit)

    # Initialize default return values
    subscriptions_data, limit, offset, total = None, None, None, None

    try:
        response = requests.get(input_url, timeout=300)
        if response.status_code == 200:
            json_data = response.json()
            subscriptions_data = json_data.get("subscriptions", [])
            limit = json_data.get("limit", limit)
            offset = json_data.get("offset", 0)
            total = json_data.get("total", 0)
            count = len(subscriptions_data)
            logging.info(
                "length of subscriptions_data: %s | limit: %s | offset: %s | total: %s | count: %s",
                count,
                limit,
                offset,
                total,
                count,
            )
        else:
            logging.error(
                "Received non-200 response status code: %s",
                response.status_code,
            )
    except Exception as error:  # pylint: disable=broad-except
        logging.error("Error querying endpoint: %s", error)

    # This return statement is now consistent across all execution paths
    return subscriptions_data, limit, offset, total
