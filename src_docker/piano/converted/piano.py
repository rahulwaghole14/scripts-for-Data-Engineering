"""
piano.py
"""

import logging
import requests


def request_send(
    url, app_api_token
):  # pylint: disable=too-many-arguments,too-many-locals
    """request send"""
    try:
        params = {"api_token": app_api_token}
        # Make the POST request
        response = requests.post(url, params=params, timeout=10)

        # Check the response
        if response.status_code == 200:
            return response.json()
        logging.info("Failed to request report: HTTP %s", response.status_code)
        return response.text
    except Exception as error:  # pylint: disable=broad-except
        logging.info("error in request_send(): %s", error)
        return None
