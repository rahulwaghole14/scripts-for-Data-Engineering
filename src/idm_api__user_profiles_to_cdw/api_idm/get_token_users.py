'''import requires'''
import json
import requests
import logging

def get_token(api_url, user_name, pass_word, api_auth):
    '''Connect to the API to get a JWT token'''
    auth_url = api_url + "/jwt/token"
    auth_data = {"username": user_name, "password": pass_word}
    auth_headers = {"Content-Type": "application/json",
                    "Authorization": api_auth,
                    "content-language": "en",
                    "Connection": "keep-alive"}
    try:
        logging.info("Getting token from API")
        gettoken = requests.get(auth_url,
                        # data=json.dumps(auth_data), # not needed anymore...
                        headers=auth_headers,
                        timeout=600)
        # print("API response:", gettoken.text) # debug token response
        getjsontoken = gettoken.json()
    except requests.exceptions.RequestException as error:
        logging.error("Error getting token from API %s", error)
        raise SystemExit(error) from error
    return getjsontoken

def get_users(api_url, params, user_name, pass_word, api_auth):
    '''Connect to the API to get users'''
    api_token = get_token(api_url, user_name, pass_word, api_auth)
    user_url = api_url + '/api/users'
    auth_headers = {"Content-Type": "application/json",
                    "Authorization": "Bearer " + api_token['token']}

    try:
        logging.info("GET request to API in process %s", params)
        resp = requests.get(user_url + params,
                        headers=auth_headers,
                        timeout=600)
        return resp.json()
    except requests.exceptions.HTTPError as errh:
        logging.info("Http Error: %s",errh)
    except requests.exceptions.ConnectionError as errc:
        logging.info("Error Connecting: %s",errc)
    except requests.exceptions.Timeout as errt:
        logging.info("Timeout Error: %s",errt)
    except requests.exceptions.RequestException as err:
        logging.info("Something went wrong with the request: %s",err)
    except json.decoder.JSONDecodeError:
        logging.info("Empty Response")
        return None
        # raise SystemExit(error) from error
