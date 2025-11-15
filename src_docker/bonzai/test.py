import json
from common.aws.aws_secret import get_secret

aws_creds = get_secret("dateam_bonzai_s3")
aws_creds_json = json.loads(aws_creds)

print(aws_creds_json)
