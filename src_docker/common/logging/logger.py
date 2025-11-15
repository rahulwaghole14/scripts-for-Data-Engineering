"""
standard logging function for all projects
"""
import os
import logging
import datetime
import inspect


def logger(name, directory, level=logging.INFO):
    """Create a 'log' directory if it doesn't already exist"""
    log_dir = directory
    year_month = datetime.datetime.now().strftime("%Y/%m")
    log_dir_year_month = os.path.join(log_dir, year_month)
    if not os.path.exists(log_dir_year_month):
        os.makedirs(log_dir_year_month)

    # Set up the logging configuration
    log_file = (
        "log_"
        + name
        + "__"
        + str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        + ".log"
    )
    log_path = os.path.join(log_dir_year_month, log_file)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )
    logger = logging.getLogger(name)
    return logger


def log_start(logger, process_name=None):
    start_marker = "=" * 10
    if process_name is None:
        # Get the name of the calling module if process_name is not provided
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        process_name = module.__name__ if module else "Unknown Process"
    logger.info(f"{start_marker} SCRIPT STARTED: {process_name} {start_marker}")

def log_end(logger, process_name=None):
    end_marker = "=" * 10
    if process_name is None:
        # Get the name of the calling module if process_name is not provided
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        process_name = module.__name__ if module else "Unknown Process"
    logger.info(f"{end_marker} SCRIPT ENDED SUCCESSFULLY: {process_name} {end_marker}")