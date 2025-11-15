# pylint: disable=all

import requests
import json
from ..common import BASE_URL, REQUEST_HEADERS


def segment_json(name, description, rsid, owner, definition):
    a = json.dumps(
        {
            "id": "null",
            "name": name,
            "description": description,
            "rsid": rsid,
            "owner": {"id": owner},
            "definition": definition,
        }
    )
    return a


def create_segment(rsid, owner, name, description, definition):

    url = BASE_URL + "segments/" + "?locale=en_US&expansion=definition"

    segment_data = segment_json(name, description, rsid, owner, definition)

    response = requests.post(
        url=url, headers=REQUEST_HEADERS, data=segment_data
    )

    return response


def segment_info(request_body, segment):

    request_url = BASE_URL + "segments/" + segment

    response = requests.get(
        url=request_url, headers=REQUEST_HEADERS, params=request_body
    )

    json_response = response.json()

    return json_response


def update_segment(rsid, owner, segment, name, description, definition):

    url = (
        BASE_URL + "segments/" + segment + "?locale=en_US&expansion=definition"
    )

    segment_data = segment_json(name, description, rsid, owner, definition)

    response = requests.put(
        url=url, headers=REQUEST_HEADERS, data=segment_data
    )

    return response
