
import execjs
import json

# JavaScript code as a string
js_code = """
const CryptoJS = require('crypto-js');

function getPPID(yob, email) {
  const ppid = `${yob}/${email}`;
  return hashPPID(ppid);
}

function hashPPID(ppid) {
  // Handle plain text PPID directly without URL decoding
  const base64Decoded = CryptoJS.enc.Base64.parse(ppid);
  return CryptoJS.SHA512(base64Decoded).toString(CryptoJS.enc.Hex);
}
"""

# Load JavaScript context
ctx = execjs.compile(js_code)


def get_cookie_by_key(cookies, key):
    cookie = next(
        (item for item in cookies if item["cookie_key"] == key), None
    )
    return cookie["cookie_value"] if cookie else None


def get_ppid(cookies):
    acc_cookie = get_cookie_by_key(cookies, "account")
    if acc_cookie:
        account = json.loads(acc_cookie)
        print(f"Account JSON: {account}")
        ppid = f"{account['yob']}/{account['email']}"
        print(
            "Concatenated PPID:", ppid
        )  # Log the concatenated string for comparison
        hashed_ppid = ctx.call("getPPID", account["yob"], account["email"])
        return hashed_ppid
    else:
        raise ValueError("Account cookie not found")


# Example input
cookies = [
    {
        "cookie_key": "account",
        "cookie_value": '{"yob": "1999", "email": "x.x@hexa.co.nz"}',
    }
]

# Run the function and print the result
try:
    for cookie in cookies:
        ppid = get_ppid(cookies)
        print("PPID:", ppid)
except Exception as e:
    print(e)
