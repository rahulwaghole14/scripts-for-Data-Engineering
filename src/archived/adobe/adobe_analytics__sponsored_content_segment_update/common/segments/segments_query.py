"""
segments query
"""
import json
import requests
import os
import logging


def create_segment(rsid, owner, name, description, definition):
    """
    create segment
    """
    try:
        url = BASE_URL + "segments/" + "?locale=en_US&expansion=definition"

        segment_data = json.dumps(
            {
                "id": "null",
                "name": name,
                "description": description,
                "rsid": rsid,
                "owner": {"id": owner},
                "definition": definition,
            }
        )

        response = requests.post(
            url=url, headers=REQUEST_HEADERS, data=segment_data, timeout=100
        )
    except Exception as error:
        logging.info("error in create_segment(): %s", error)

    return response


def segment_info(request_body, segment):
    """
    segment info
    """
    request_url = BASE_URL + "segments/" + segment

    response = requests.get(
        url=request_url,
        headers=REQUEST_HEADERS,
        params=request_body,
        timeout=100,
    )

    json_response = response.json()

    return json_response


def update_segment(
    rsid, owner, segment, name, description, base_url, headers, definition
):
    """
    update adobe analytics segment
    """

    try:
        url = (
            base_url
            + "segments/"
            + segment
            + "?locale=en_US&expansion=definition"
        )
        segment_data = json.dumps(
            {
                "id": "null",
                "name": name,
                "description": description,
                "rsid": rsid,
                "owner": {"id": owner},
                "definition": definition,
            }
        )

        response = requests.put(
            url=url, headers=headers, data=segment_data, timeout=100
        )
        if response.status_code == 200:
            logging.info("Segment updated successfully")
        else:
            logging.error(
                f"Failed to update segment. Status code: {response.status_code}, Response: {response.text}"
            )

    except Exception as error:
        logging.info("error in update_segment(): %s", error)

    return response
