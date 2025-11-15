import logging
import boto3
import time


def configure_logging():
    # Set up logging
    logging.basicConfig(level=logging.INFO)


def send_log_to_cloudwatch(message):
    # Create a CloudWatch Logs client
    client = boto3.client("logs")

    # Send log message to CloudWatch Logs
    response = client.put_log_events(
        logGroupName="docker_eks_log",  # Replace with your log group name
        logStreamName="test",
        logEvents=[
            {"timestamp": int(round(time.time() * 1000)), "message": message}
        ],
    )
