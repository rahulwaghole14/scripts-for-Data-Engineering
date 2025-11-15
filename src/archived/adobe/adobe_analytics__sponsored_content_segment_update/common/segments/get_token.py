""" get short lived token from adobe """
import json
import time

import jwt
import requests

# "SDK_URL":"https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk",
# "JWT_URL":"https://ims-na1.adobelogin.com/ims/exchange/jwt/",


def get_token(cred, key):
    """get token"""
    seconds = time.time()
    # set seconds to 10 mins in future
    seconds = round(seconds + 60 * 10)
    private_key = key
    payload = {
        "exp": seconds,
        "iss": cred["ISS"],
        "sub": cred["SUB"],
        "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": True,
        # cred['SDK_URL']: True,
        "aud": cred["AUD"],
    }
    encoded = jwt.encode(payload, private_key, algorithm="RS256")
    url = "https://ims-na1.adobelogin.com/ims/exchange/jwt/"
    # url = cred['JWT_URL']
    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "client_id": cred["X_API_KEY"],
        "client_secret": cred["CLIENT_SECRET"],
        "jwt_token": encoded,
    }
    response = requests.post(url=url, headers=headers, data=body, timeout=30)
    token = json.loads(response.text)
    return token
