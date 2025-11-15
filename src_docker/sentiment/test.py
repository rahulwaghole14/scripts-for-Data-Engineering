from .drupal.drupal import drupal_run
from common.aws.aws_secret import get_secret
from common.aws.aws_sns import publish_message_to_sns
import logging

try:
    sentiment_drupal = get_secret("datateam_sentiment")
    logging.info("Running drupal task with ")
    DRUPAL_CONTENT_API_PRD = "https://platform-api.hexa.co.nz/api/"
    # API_ENDPOINT = "https://platform-api.hexa.co.nz/api/"
    drupal_run(sentiment_drupal, DRUPAL_CONTENT_API_PRD)
    logging.info(f"Drupal task completed with {DRUPAL_CONTENT_API_PRD}")
    publish_message_to_sns(
        "datateam_eks_notifications", "Drupal task completed"
    )
except Exception as e:
    publish_message_to_sns(
        "datateam_eks_notifications", f"Error running drupal task: {e}"
    )
    logging.error(f"Error running drupal task: {e}")

try:
    sentiment_drupal = get_secret("datateam_sentiment")
    logging.info("Running drupal task with ")
    API_ENDPOINT = "https://platform-api.hexa.co.nz"
    drupal_run(sentiment_drupal, API_ENDPOINT)
    logging.info(f"Drupal task completed with {API_ENDPOINT}")
    publish_message_to_sns(
        "datateam_eks_notifications", "Drupal task completed"
    )
except Exception as e:
    publish_message_to_sns(
        "datateam_eks_notifications", f"Error running drupal task: {e}"
    )
    logging.error(f"Error running drupal task: {e}")

try:
    sentiment_drupal = get_secret("datateam_sentiment")
    logging.info("Running drupal task with ")
    API_ENDPOINT = "172.20.202.70"
    drupal_run(sentiment_drupal, API_ENDPOINT)
    logging.info(f"Drupal task completed with {API_ENDPOINT}")
    publish_message_to_sns(
        "datateam_eks_notifications", "Drupal task completed"
    )
except Exception as e:
    publish_message_to_sns(
        "datateam_eks_notifications", f"Error running drupal task: {e}"
    )
    logging.error(f"Error running drupal task: {e}")
