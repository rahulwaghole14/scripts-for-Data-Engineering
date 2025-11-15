import os
import json
from dotenv import load_dotenv
#### need to change the table to right one we need for this project
def load_config():
    load_dotenv()  # Load environment variables from .env file.
    config = {
        "google_cred": json.loads(os.getenv("GOOGLE_CLOUD_CRED")),
        "project_id": os.getenv("GOOGLE_CLOUD_PROJECT_ID", 'default-project-id')
    }
    return config


