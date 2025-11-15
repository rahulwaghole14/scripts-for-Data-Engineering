import logging
import boto3
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-2"  # Specify your AWS region


def get_topic_arn_by_name(topic_name):
    """
    Retrieves the ARN of an SNS topic by its name.

    :param topic_name: The name of the SNS topic.
    :return: The ARN of the SNS topic if found, otherwise None.
    """
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        response = sns.list_topics()
        for topic in response["Topics"]:
            if topic_name in topic["TopicArn"]:
                return topic["TopicArn"]
        logging.error("Topic not found")
        return None

    except Exception as e:
        logging.error("Error retrieving topic ARN: %s", e)
        return None


def publish_message_to_sns(topic_name, message):
    """
    Publishes a message to the specified Amazon SNS topic by name.

    :param topic_name: The name of the Amazon SNS topic.
    :param message: The message to be published.
    :return: The response from the publish operation if successful, otherwise None.
    """
    try:
        topic_arn = get_topic_arn_by_name(topic_name)
        if not topic_arn:
            logging.error("Failed to get topic ARN")
            return None

        sns = boto3.client("sns", region_name=AWS_REGION)
        response = sns.publish(TopicArn=topic_arn, Message=message)
        logging.info("Message sent successfully: %s", response["MessageId"])
        return response
    except Exception as e:
        logging.error("Error publishing message: %s", e)
        return None


# Example usage
if __name__ == "__main__":
    topic_name = "my-sns-topic"
    message = "Hello, this is a test message."
    publish_message_to_sns(topic_name, message)
