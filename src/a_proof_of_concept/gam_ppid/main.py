# import hashlib
# import base64
# import json
# import uuid

# # Function to get a cookie by key
# def get_cookie_by_key(cookies, key):
#     for cookie in cookies.split(';'):
#         cookie = cookie.strip()
#         if cookie.startswith(key + '='):
#             return cookie[len(key) + 1:]
#     return None

# # Function to generate a UUID
# def generate_uuid():
#     return str(uuid.uuid4())

# # Function to hash the PPID
# def hash_ppid(ppid):
#     url_decoded = base64.urlsafe_b64decode(ppid)
#     sha512_hash = hashlib.sha512(url_decoded).hexdigest()
#     return sha512_hash

# # Function to get the PPID
# def get_ppid(cookies, yob=None, email=None):
#     acc_cookie = get_cookie_by_key(cookies, 'account')
#     if acc_cookie:
#         account = json.loads(base64.urlsafe_b64decode(acc_cookie).decode('utf-8'))
#         ppid = f"{account['yob']}/{account['email']}"
#         return hash_ppid(base64.urlsafe_b64encode(ppid.encode('utf-8')).decode('utf-8'))
#     else:
#         cookie = get_cookie_by_key(cookies, 'deviceuuid')
#         if not cookie:
#             deviceuuid = generate_uuid()
#             hashed_deviceuuid = hash_ppid(base64.urlsafe_b64encode(deviceuuid.encode('utf-8')).decode('utf-8'))
#             cookie = json.dumps({'deviceuuid': hashed_deviceuuid})
#         else:
#             cookie = base64.urlsafe_b64decode(cookie).decode('utf-8')
#         return json.loads(cookie)['deviceuuid']

# # Input cookies, year of birth, and email
# cookies = 'account=' + base64.urlsafe_b64encode(json.dumps({'yob': '1999', 'email': 'x.x@hexa.co.nz'}).encode('utf-8')).decode('utf-8')
# yob = '1999'
# email = 'x.x@hexa.co.nz'

# # Test the function
# ppid = get_ppid(cookies, yob, email)
# print("PPID:", ppid)

# de06f49e7dd771ee

import hashlib
import base64
import json
import uuid
import urllib.parse

# Function to get a cookie by key
def get_cookie_by_key(cookies, key):
    for cookie in cookies.split(";"):
        cookie = cookie.strip()
        if cookie.startswith(key + "="):
            return cookie[len(key) + 1 :]
    return None


# Function to generate a UUID
def generate_uuid():
    return str(uuid.uuid4())


# Function to hash the PPID
def hash_ppid(ppid):
    # URL decode
    url_decoded = urllib.parse.unquote(ppid)
    # Base64 decode
    base64_decoded = base64.b64decode(url_decoded)
    # SHA-512 hash
    sha512_hash = hashlib.sha512(base64_decoded).hexdigest()
    # Returning a shorter version to match JavaScript output
    return sha512_hash[:16]


# Function to get the PPID
def get_ppid(cookies, yob=None, email=None):
    acc_cookie = get_cookie_by_key(cookies, "account")
    if acc_cookie:
        account = json.loads(urllib.parse.unquote(acc_cookie))
        ppid = f"{account['yob']}/{account['email']}"
        # Base64 encode for hashing
        base64_encoded_ppid = base64.b64encode(ppid.encode("utf-8")).decode(
            "utf-8"
        )
        return hash_ppid(base64_encoded_ppid)
    else:
        cookie = get_cookie_by_key(cookies, "deviceuuid")
        if not cookie:
            deviceuuid = generate_uuid()
            hashed_deviceuuid = hash_ppid(
                base64.b64encode(deviceuuid.encode("utf-8")).decode("utf-8")
            )
            cookie = json.dumps({"deviceuuid": hashed_deviceuuid})
        else:
            cookie = urllib.parse.unquote(cookie)
        return json.loads(cookie)["deviceuuid"]


# Input cookies, year of birth, and email
account_info = {"yob": "1999", "email": "x.x@hexa.co.nz"}
encoded_account = urllib.parse.quote(json.dumps(account_info))
cookies = f"account={encoded_account}"

# Test the function
ppid = get_ppid(cookies)
print("PPID:", ppid)
