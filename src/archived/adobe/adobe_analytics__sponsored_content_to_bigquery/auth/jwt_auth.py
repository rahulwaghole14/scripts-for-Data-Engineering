import requests
import jwt
import time
import json
import os
from common import SECRET_PATH

secrets_file = os.path.join(str(SECRET_PATH), "secret_values.json")

with open(secrets_file, "r") as file:
    secret_values = json.load(file)

# print(secret_values)

X_API_KEY = secret_values["X_API_KEY"]
CLIENT_SECRET = secret_values["CLIENT_SECRET"]
ISS = secret_values["ISS"]
SUB = secret_values["SUB"]
AUD = secret_values["AUD"]

# print(X_API_KEY)
# print(CLIENT_SECRET)
# print(ISS)
# print(SUB)
# print(AUD)


def auth_jwt():
    seconds = time.time()
    # set seconds to 5 mins in future
    seconds = round(seconds + 60 * 5)

    key_file = os.path.join(str(SECRET_PATH), "private.key")

    with open(key_file, "r") as myfile:
        private_key = myfile.read()

    payload = {
        "exp": seconds,
        "iss": ISS,
        "sub": SUB,
        "https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": True,
        "aud": AUD,
    }

    encoded = jwt.encode(payload, private_key, algorithm="RS256")

    url = "https://ims-na1.adobelogin.com/ims/exchange/jwt/"

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    body = {
        "client_id": X_API_KEY,
        "client_secret": CLIENT_SECRET,
        "jwt_token": encoded,
    }

    response = requests.post(url=url, headers=headers, data=body)

    json_data = json.loads(response.text)

    with open(str(SECRET_PATH) + "\\access.json", "w") as outfile:
        json.dump(json_data, outfile)
