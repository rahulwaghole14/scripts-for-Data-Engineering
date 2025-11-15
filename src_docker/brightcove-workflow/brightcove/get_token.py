""" get token from Brightcove API """
import logging
import os
import json
from pydoc import cli

import requests
from common.aws.aws_secret import get_secret


def get_token():
    """get token object from Brightcove API"""
    # CORS enablement and other headers
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Content-type": "application/json",
        "X-Content-Type-Options": "nosniff",
        "X-XSS-Protection": "1; mode=block",
    }

    try:
        # note that if you are using this proxy for a single credential
        # you can just hardcode the client id and secret below instead of passing them

        secret_name = "brightcove"
        secret_value = get_secret(secret_name)
        data = json.loads(secret_value)

        client_id = data["BRIGHTCOVE_CLIENT_ID"]
        client_secret = data["BRIGHTCOVE_CLIENT_SECRET"]
        request_url = data["BRIGHTCOVE_TOKEN_URL"]

        # client_id = os.environ.get("BRIGHTCOVE_CLIENT_ID")
        # client_secret = os.environ.get("BRIGHTCOVE_CLIENT_SECRET")
        # request_url = os.environ.get("BRIGHTCOVE_TOKEN_URL")

        # send POST request
        response = requests.post(
            url=request_url + "?grant_type=client_credentials",
            auth=(client_id, client_secret),
            headers=headers,
            timeout=30,
        )

        # Check for errors
        if response.status_code != 200:
            response.raise_for_status()
            return None

        logging.info("Token request successful in get_token() function")

    except Exception as error:  # pylint: disable=broad-except
        logging.info("Error in get_token() function: %s", error)
        return None

    return response.json()
