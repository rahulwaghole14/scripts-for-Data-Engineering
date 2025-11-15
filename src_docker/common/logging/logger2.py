''' logger '''
import datetime
import logging
import os

def get_logger(name):
    '''Create a "log" directory if it doesn't already exist'''
    log_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log')
    year_month = datetime.datetime.now().strftime('%Y/%m')
    log_dir_year_month = os.path.join(log_dir, year_month)
    if not os.path.exists(log_dir_year_month):
        os.makedirs(log_dir_year_month)

    # Set up the logging configuration
    log_file = 'log_' + name + '__' + str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')) + '.log'
    log_path = os.path.join(log_dir_year_month, log_file)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

    # File Handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream Handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Prevent logging from propagating to the root logger
    logger.propagate = False

    return logger
