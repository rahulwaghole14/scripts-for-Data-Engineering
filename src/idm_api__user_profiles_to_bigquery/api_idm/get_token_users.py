"""import requires"""
import logging
import requests


def get_token(api_url, api_auth):
    """Connect to the API to get a JWT token"""
    auth_url = api_url + "/jwt/token"
    # auth_data = {"username": user_name, "password": pass_word}
    auth_headers = {
        "Content-Type": "application/json",
        "Authorization": api_auth,
        "content-language": "en",
        "Connection": "keep-alive",
    }
    try:
        logging.info("Getting token from API")
        gettoken = requests.get(
            auth_url,
            # data=json.dumps(auth_data), # not needed anymore...
            headers=auth_headers,
            timeout=600,
        )
        # print("API response:", gettoken.text) # debug token response
        getjsontoken = gettoken.json()
    except requests.exceptions.RequestException as error:
        logging.error("Error getting token from API %s", error)
        raise SystemExit(error) from error
    return getjsontoken


def get_users(api_url, params, api_auth):
    """Connect to the API to get users"""
    api_token = get_token(api_url, api_auth)
    user_url = api_url + "/api/users"
    auth_headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + api_token["token"],
    }

    try:
        logging.info("GET request API URL: %s", user_url + params)
        resp = requests.get(
            user_url + params, headers=auth_headers, timeout=600
        )
        return resp.json()
    except Exception as error:  # pylint: disable=broad-except
        logging.error("Error getting users from API %s", error)
        raise SystemExit(error) from error
