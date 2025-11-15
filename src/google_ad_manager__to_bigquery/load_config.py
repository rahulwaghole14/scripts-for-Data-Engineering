"""load config file"""
import os
from dotenv import load_dotenv

#### need to change the table to right one we need for this project
def load_config():
    """
    Load configuration from environment variables.
    """
    load_dotenv()  # Load environment variables from .env file.
    config = {
        "google_cred": os.environ.get("GOOGLE_CLOUD_CRED_BASE64"),
        "project_id": os.getenv(
            "GAM_GOOGLE_CLOUD_PROJECT_ID", "default-project-id"
        ),  # Use default if not set
        "dataset_id": os.getenv("GAM_BIGQUERY_DATASET_ID"),
        "api_version": os.environ.get("GAM_API_VERSION", "v202311"),
        "PCA_GEN_QUERY": str(os.getenv("GAM_PCA_GEN_QUERY")),
        "PCA_HOUR_QUERY": int(os.getenv("GAM_PCA_HOUR_QUERY")),
        "PCA_VIEW_QUERY": int(os.getenv("GAM_PCA_VIEW_QUERY")),
        "PCA_VIEW_101_QUERY": int(os.getenv("GAM_PCA_VIEW_101_QUERY")),
        "PCA_VIEW_102_QUERY": int(os.getenv("GAM_PCA_VIEW_102_QUERY")),
        "PCA_VIEW_103_QUERY": int(os.getenv("GAM_PCA_VIEW_103_QUERY")),
        "VV_GEN_QUERY": int(os.getenv("GAM_VV_GEN_QUERY")),
        "VV_SELLTHROUGH_QUERY": int(os.getenv("GAM_VV_SELLTHROUGH_QUERY")),
        "SPON_CON_QUERY": int(os.getenv("SPON_CON_QUERY", "0")),
        "FILL_RATE": int(os.getenv("GAM_FILL_RATE")),
        "VIDEO_VIEWERSHIP": int(os.getenv("GAM_VIDEO_VIEWERSHIP")),
        "backfill_days": 10,
    }
    return config


load_config()
