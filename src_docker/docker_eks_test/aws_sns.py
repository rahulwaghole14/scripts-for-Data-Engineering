import boto3


def publish_message_to_sns(topic_arn, message):
    """
    Publishes a message to the specified Amazon SNS topic.

    :param topic_arn: The ARN of the Amazon SNS topic.
    :param message: The message to be published.
    :return: The response from the publish operation if successful, otherwise None.
    """
    try:
        # Initialize the SNS client
        sns = boto3.client("sns")

        # Publish the message to the specified SNS topic
        response = sns.publish(TopicArn=topic_arn, Message=message)
        print("Message sent successfully:", response["MessageId"])
        return response
    except Exception as e:
        print("Error publishing message:", e)
        return None


# Example usage:
# topic_arn = 'arn:aws:sns:ap-southeast-2:190748537425:test'
# message = 'Hello from Python!'
# publish_message_to_sns(topic_arn, message)
