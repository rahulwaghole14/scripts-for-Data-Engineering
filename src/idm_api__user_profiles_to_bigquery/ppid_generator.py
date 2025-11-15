import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import execjs
import logging
import base64
import urllib

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ppid creation function
js_code = """
const CryptoJS = require('crypto-js');

function hashPPID(ppid) {
  // Handle plain text PPID directly without URL decoding
  const urlDecoded = decodeURIComponent(ppid);
  const base64Decoded = CryptoJS.enc.Base64.parse(urlDecoded);
  return CryptoJS.SHA512(base64Decoded).toString(CryptoJS.enc.Hex);
}
"""

# Load JavaScript context
ctx = execjs.compile(js_code)


def generate_ppid_js(ppid_base):
    """input ppid_base: str as account_id
    output: hashed ppid
    """
    try:
        return ctx.call("hashPPID", ppid_base)
    except Exception as e:
        logging.info("Error in hashppid: %s", e)


def pad_base64(base64_string):
    """Add padding to a base64 string to make its length a multiple of 4."""
    return base64_string + "=" * (-len(base64_string) % 4)


def generate_ppid(ppid_base):
    """Hash PPID using SHA-512 directly."""
    try:
        url_decoded = urllib.parse.unquote(
            ppid_base
        )  # Decode URL-encoded string
        padded_base64 = pad_base64(url_decoded)  # Ensure proper padding
        base64_decoded = base64.b64decode(padded_base64)  # Decode base64
        sha512_hashed = hashlib.sha512(
            base64_decoded
        ).hexdigest()  # Hash with SHA-512
        return sha512_hashed
    except Exception as e:
        # logging.info("Error in hash_ppid (Python): %s. Falling back to JS.", e)
        return generate_ppid_js(ppid_base)


def process_user(user):
    ppid = generate_ppid(str(user["id"]))
    user["ppid"] = ppid
    # logging.info("py Generated PPID for user: %s, %s", user["id"], ppid)
    return user


def generate_ppids_concurrently(
    users, max_workers=1
):  # limit to 1 default worker
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_user, user) for user in users]
        results = []
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    logging.info("Generated PPIDs for %s users", len(results))
    return results
