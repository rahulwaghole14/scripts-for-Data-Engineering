""" define get article functions """
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.auth import HTTPBasicAuth



def get_content(session, endpoint, path, query, user, pwd):
    """ get the list of content """

    full_url = endpoint + path + query

    try:
        content = session.get(full_url, timeout=10, auth=HTTPBasicAuth(user, pwd), headers={
            'accept': 'application/json'
        })

    except Exception as error:
        logging.error("get_content() an exception occured: %s", error)

    return content

def get_content_metadata(session, endpoint, path, contentid, main_pub_key, user, pwd):
    """ get the metadata of single content id """

    full_url = endpoint + path + contentid + f"?publication_channel={main_pub_key}"

    try:
        content = session.get(full_url, timeout=10, auth=HTTPBasicAuth(user, pwd), headers={
            'accept': 'application/json'
        })

        # Check for 'AccessDenied' in the response
        if content.status_code == 403:  # 403 is usually used for access denied errors
            error_data = content.json()
            if error_data.get("code") == "AccessDenied":
                logging.info(
                    "get_content_metadata(): Access denied for content ID %s: %s", contentid, error_data.get("message")
                    )
                return None  # Or handle as appropriate

    except Exception as error:
        logging.error("get_content_metadata() an exception occured: %s", error)

    return content

# Custom HTTPAdapter
class LoggingHTTPAdapter(HTTPAdapter):
    """ create a class for a logging adapter for http requests """
    def send(self, request, **kwargs):
        retries = kwargs.get('retries')
        if retries:
            logging.info(f"Retry attempt: {retries.total_retries - retries.total - 1}")
        return super().send(request, **kwargs)


def init_session():
    ''' http session setup '''
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = LoggingHTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
