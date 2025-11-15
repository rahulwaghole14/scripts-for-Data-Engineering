import json

from common.aws.aws_secret import get_secret

#### need to change the table to right one we need for this project


def load_config():
    google_cred = get_secret("datateam_google_prod")
    data = json.loads(google_cred)
    project_id = data["project_id"]

    config = {"google_cred": data, "project_id": project_id}
    return config
