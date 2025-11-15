"""
load secret from aws secret manager
"""

import json
import yaml
from common.aws.aws_secret import get_secret


def load_config():
    """
    Load configuration from environment variables.
    """

    gam_keyfile = get_secret("datateam_admanager_keyfile")
    gam_yaml = get_secret("datateam_admanager_yaml")
    google_prod = get_secret("datateam_google_prod")
    data = json.loads(google_prod)

    google_cred_secret = json.loads(get_secret("GOOGLE_CLOUD_CRED_BASE64"))[
        "GOOGLE_CLOUD_CRED_BASE64"
    ]

    admanager = get_secret("datateam_admanager")
    admanager_data = json.loads(admanager)

    config = {
        "google_cred": google_cred_secret,
        "project_id": data["project_id"],  # Use default if not set
        "dataset_id": admanager_data["BIGQUERY_DATASET_ID"],
        "api_version": admanager_data["API_VERSION"],
        "PCA_GEN_QUERY": admanager_data["PCA_GEN_QUERY"],
        "PCA_HOUR_QUERY": int(admanager_data["PCA_HOUR_QUERY"]),
        "PCA_VIEW_QUERY": int(admanager_data["PCA_VIEW_QUERY"]),
        "PCA_VIEW_101_QUERY": int(admanager_data["PCA_VIEW_101_QUERY"]),
        "PCA_VIEW_102_QUERY": int(admanager_data["PCA_VIEW_102_QUERY"]),
        "PCA_VIEW_103_QUERY": int(admanager_data["PCA_VIEW_103_QUERY"]),
        "VV_GEN_QUERY": int(admanager_data["VV_GEN_QUERY"]),
        "VV_SELLTHROUGH_QUERY": int(admanager_data["VV_SELLTHROUGH_QUERY"]),
        "SPON_CON_QUERY": int(admanager_data["SPON_CON_QUERY"]),
        "FILL_RATE": int(admanager_data["FILL_RATE"]),
        "VIDEO_VIEWERSHIP": int(admanager_data["VIDEO_VIEWERSHIP"]),
        "gam_keyfile": json.loads(gam_keyfile),
        "gam_yaml": yaml.safe_load(gam_yaml)["ad_manager"],
        "backfill_days": 10,
    }
    return config
