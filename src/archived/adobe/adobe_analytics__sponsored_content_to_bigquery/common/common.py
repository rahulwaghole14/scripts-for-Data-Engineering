# pylint: disable=all

import json
import os
import base64
import logging
from dotenv import load_dotenv
from .paths import SECRET_PATH
from .get_token import get_token

load_dotenv()

DF_COLUMNS = 1
DF_ROWS = 0

encoded_cred = os.environ.get("ADOBECRED_SPONCON_ENV")
decoded_cred = base64.b64decode(encoded_cred).decode("utf-8")
secret_values = json.loads(decoded_cred)

X_API_KEY = secret_values["X_API_KEY"]
logging.info("apikey set: %s", X_API_KEY)

with open(os.path.join(str(SECRET_PATH), "access.json")) as json_file:
    data = json.load(json_file)

project_id = "hexa-data-report-etl-prod"
dataset_id = "sponsored_content"
key_path = "src/adobe_analytics__sponsored_content_to_bigquery/hexa-data-report-etl-prod-5b93b81b644e.json"


with open(os.path.join(SECRET_PATH, "private.key"), "r") as key_file:
    private_key = key_file.read()

adobe_file_path = os.path.join(str(SECRET_PATH), "adobepriv.data")

# Open the JSON file
with open(adobe_file_path, "r", encoding="utf-8") as file:
    # Parse the JSON file
    private_key = file.read()
    print("private_key")

token = get_token(
    secret_values, private_key
)  # ACCESS_TOKEN = data['access_token']
ACCESS_TOKEN = token["access_token"]
print("ACCESS_TOKEN")

COMPANY_ID = "fairfa5"

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36",
    "Accept": "application/json",
    "Authorization": "Bearer " + ACCESS_TOKEN,
    "Content-Type": "application/json",
    "x-api-key": X_API_KEY,
    "x-proxy-global-company-id": COMPANY_ID,
}

BASE_URL = "https://analytics.adobe.io/api/" + COMPANY_ID + "/"
