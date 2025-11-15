import requests
import json
import os
from common import SECRET_PATH


def generate_oauth():
    secrets_file = os.path.join(str(SECRET_PATH), 'client_id.json')

    with open(secrets_file, 'r') as file:
        secret_values = json.load(file)

    CLIENT_ID = secret_values['client_id']
    CLIENT_SECRET = secret_values['client_secret']


    URL = 'https://ims-na1.adobelogin.com/ims/token/v3' 

    HEADERS = {'Content-Type': "application/x-www-form-urlencoded"}

    DATA = {
        'grant_type':'client_credentials',
        'client_id':CLIENT_ID,
        'client_secret':CLIENT_SECRET,
        'scope':'openid,AdobeID,additional_info.projectedProductContext'
    }


    x = requests.post(url=URL,headers=HEADERS,data=DATA)

    z = json.loads(x.text)

    with open(str(SECRET_PATH) + '\\access.json', 'w') as outfile:
            json.dump(z, outfile)


