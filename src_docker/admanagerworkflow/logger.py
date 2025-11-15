import logging


def logger(name):
    """Set up logging to display logs on the command prompt, Docker logs, or EKS logs"""

    # Set up the logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[logging.StreamHandler()],
    )
