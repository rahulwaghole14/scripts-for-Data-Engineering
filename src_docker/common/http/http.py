"""
http.py for setting up http session and retry logic
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def init_session():
    """
    Initialize a requests session with retry logic.
    """
    session = requests.Session()

    # Define the retry strategy
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=0.3,  # A backoff factor to apply between attempts
        status_forcelist=[500, 502, 503, 504],  # HTTP status codes to retry on
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Methods to retry on
        raise_on_status=False,  # Don't raise exceptions on status codes
    )

    # Create an adapter with the retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Mount the adapter to both HTTP and HTTPS protocols
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
