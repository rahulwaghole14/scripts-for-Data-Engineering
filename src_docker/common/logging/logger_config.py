"""
This module provides a function to set up a
logger using a configuration file.
"""
import logging
import logging.config
import json
import os


def setup_logger(name, log_file_name):
    """
    Sets up and returns a logger with the given name using the configuration
    defined in logging_config.json. Allows specifying a custom log file name.

    Parameters:
    - name: The name of the logger.
    - log_file_name: The filename for the log file.
    """
    # Assuming this script is inside the 'a_common' package, directly use the target path
    config_path = os.path.join(
        os.path.dirname(__file__), "logging_config.json"
    )
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

        # Directly set the log directory relative to the current script's location
        log_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "logs")
        )
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, log_file_name)
        config["handlers"]["fileHandler"]["filename"] = log_file_path

        logging.config.dictConfig(config)

    return logging.getLogger(name)
