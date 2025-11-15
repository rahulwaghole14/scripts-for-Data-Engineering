""" logger """
import logging
from datetime import date

# pylint: disable=all


def logger():
    """logger"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler(
                ".\\src\\adobe_analytics__sponsored_content_to_bigquery\\log\\"
                + "sponsored_content_"
                + str(date.today())
                + ".log"
            ),
            logging.StreamHandler(),
        ],
    )
