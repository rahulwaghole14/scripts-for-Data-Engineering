import requests

def get_token(client_id, client_secret):
    url = 'https://ims-na1.adobelogin.com/ims/token/v3'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'openid, AdobeID, additional_info.projectedProductContext'
    }
    
    response = requests.post(url, headers=headers, data=data, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    print(f"Token type: {token_data.get('token_type')}")
    print(f"Expires in: {token_data.get('expires_in')} seconds")
    print(f"Scope: {token_data.get('scope')}")
    return token_data