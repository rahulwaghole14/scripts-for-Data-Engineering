import os
import logging
import datetime


def logger(name):
    """Set up logging to display logs on the command prompt, Docker logs, or EKS logs"""

    # Set up the logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[logging.StreamHandler()],
    )


# def logger(name):
#     '''Create a "log" directory if it doesn't already exist'''
#     log_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log')
#     year_month = datetime.datetime.now().strftime('%Y/%m')
#     log_dir_year_month = os.path.join(log_dir, year_month)
#     if not os.path.exists(log_dir_year_month):
#         os.makedirs(log_dir_year_month)

#     # Set up the logging configuration
#     log_file = 'log_' + name + '__' + str(datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')) + '.log'
#     log_path = os.path.join(log_dir_year_month, log_file)
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
#         handlers=[
#             logging.FileHandler(log_path),
#             logging.StreamHandler()
#         ])
