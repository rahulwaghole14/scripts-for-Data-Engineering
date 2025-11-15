import boto3
from botocore.exceptions import (
    NoCredentialsError,
    PartialCredentialsError,
    ClientError,
)


def list_files_in_s3_bucket(
    bucket_name, prefix, aws_access_key_id, aws_secret_access_key, region_name
):
    """
    Lists all the files in an S3 bucket . Optionally, a prefix can be provided to list only files under a specific folder.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str, optional): Prefix to list files under a specific path (folder). Defaults to None.
        aws_access_key_id (str, optional): AWS access key ID.
        aws_secret_access_key (str, optional): AWS secret access key.
        region_name (str, optional): AWS region. Defaults to 'us-east-1'.

    Returns:
        List of file keys (paths) in the S3 bucket, or None if an error occurs.
    """
    try:
        # Initialize the S3 client with the provided credentials
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        # List objects in the specified bucket with the optional prefix
        file_list = []
        continuation_token = None

        while True:
            # Fetch list of objects in S3 bucket with pagination
            if continuation_token:
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=continuation_token,
                )
            else:
                response = s3.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix
                )

            # Check if there are contents in the response
            if "Contents" in response:
                for obj in response["Contents"]:
                    file_list.append(
                        obj["Key"]
                    )  # Append the file key (file path) to the list

            # Check if there are more files to list (pagination)
            if response.get(
                "IsTruncated"
            ):  # If True, there are more files to fetch
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return file_list

    except NoCredentialsError:
        print("Error: AWS credentials not provided or are invalid.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AccessDenied":
            print(f"Error: Access denied to bucket '{bucket_name}'.")
        elif error_code == "NoSuchBucket":
            print(f"Error: The bucket '{bucket_name}' does not exist.")
        else:
            print(f"Unexpected error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None

