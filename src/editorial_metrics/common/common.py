import json
import os
from common.paths import SECRET_PATH


secrets_file = os.path.join(str(SECRET_PATH), 'secret_values.json')

with open(secrets_file, 'r') as file:
    secret_values = json.load(file)

X_API_KEY = secret_values['X_API_KEY']


with open(os.path.join(str(SECRET_PATH), 'access.json')) as json_file:
    data = json.load(json_file)

if 'access_token' in data:
    ACCESS_TOKEN = data['access_token']
else:
    ACCESS_TOKEN = 'A'

COMPANY_ID = 'fairfa5'

REQUEST_HEADERS = {
    'Accept': 'application/json',
    'Authorization': 'Bearer ' + ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'x-api-key': X_API_KEY,
    'x-proxy-global-company-id': COMPANY_ID
}

BASE_URL = 'https://analytics.adobe.io/api/' + COMPANY_ID + '/'


