"""
load_config.py: Load configuration from .env file.
"""
import os
import json
from dotenv import load_dotenv


def load_config():
    """
    load_config: Load configuration from .env file.
    """
    load_dotenv()  # Load environment variables from .env file.

    config = {
        "GOOGLE_CLOUD_CRED": json.loads(os.getenv("GOOGLE_CLOUD_CRED")),
        "PROJECT_ID": "hexa-data-report-etl-prod",
        "DATASET_ID": "prod_dw_staging",
        "TABLE_ID": "stg_idm__user_profiles",
        "INITIAL_DATE": "2023-12-05",  # day1 we start the data loading to S3
    }

    return config
