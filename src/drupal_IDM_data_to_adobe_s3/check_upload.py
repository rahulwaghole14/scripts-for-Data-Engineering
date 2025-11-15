import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def check_and_print_file_content(bucket, key_prefix):
    s3 = boto3.client('s3')
    try:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix)
        for obj in objects.get('Contents', []):
            file_key = obj['Key']
            print(f"Found file: {file_key}")

            # Fetch and print file content
            response = s3.get_object(Bucket=bucket, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            print(f"Content of {file_key}:\n{content}\n")

        return len(objects.get('Contents', [])) > 0
    except Exception as e:
        print(f"Error: {e}")
        return False

bucket_name = os.getenv('S3_BUCKET')
key_prefix = os.getenv("S3_KEY_PREFIX")
file_exists = check_and_print_file_content(bucket_name, key_prefix)
print(f"File exists: {file_exists}")
