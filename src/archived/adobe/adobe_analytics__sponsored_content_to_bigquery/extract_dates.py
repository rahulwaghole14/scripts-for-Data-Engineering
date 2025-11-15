"""
Author: Deepak Kumar Singh
"""

import gc
import glob
import os
import re
import sys
import logging
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter


def extract_date_from_log(path):
    """
    Extracting start and end dates from log files if any. The start and end dates are passed to Adobe API 2.0 to
    extract data.

    param path: Path of log files
    type path: string

    returns: Start and End dates of each table that are: mobile_device, & regional
    rtype: dictionary
    """
    files = glob.glob(path + "sponsored_content_*.log")
    # logging.info(files)

    if len(files) < 1:
        logging.info("No Log Files in the path")
        sys.exit(1)

    string_check = "Appending DataFrame with"
    tables = ["mobile_device", "regional"]

    files = [os.path.split(file)[1] for file in files]
    logging.info(files)
    dates = [re.search(r"\d{4}-\d{2}-\d{2}", file) for file in files]
    dates = [datetime.strptime(dt.group(), "%Y-%m-%d").date() for dt in dates]
    dates = list(set(dates))
    # logging.info(dates)
    # descending sort of dates
    dates.sort(reverse=True)
    # logging.info(dates)

    if len(dates) < 1:
        logging.info("No dates extracted from the log files in the path")
        sys.exit(1)

    if (
        os.path.getsize(path + "sponsored_content_" + str(dates[0]) + ".log")
        > 1
    ):
        # opening the recent log file
        f = open(path + "sponsored_content_" + str(dates[0]) + ".log", "r")
    else:
        # opening the recent log file
        f = open(path + "sponsored_content_" + str(dates[1]) + ".log", "r")

    del files, dates

    lines = f.readlines()
    line_list = list()
    for line in lines:
        if string_check in line:
            line_list.append(line)
    f.close()

    extract_table = [
        (index, table)
        for table in tables
        for index, line in enumerate(line_list)
        if table in line
    ]

    line_list = [(line_list[x], y) for x, y in extract_table]
    # logging.info(line_list)
    line_list = [(ln[0].split("@")[-1], ln[1]) for ln in line_list]
    line_list = [(ln[0].strip(), ln[1].strip()) for ln in line_list]
    # logging.info(line_list)
    line_list = [ln for ln in line_list if len(ln[0]) < 11]
    line_list = [
        (datetime.strptime(ln[0], "%Y-%m-%d").date(), ln[1])
        for ln in line_list
        if bool(datetime.strptime(ln[0], "%Y-%m-%d"))
    ]
    tmp = groupby(sorted(line_list, key=itemgetter(1)), key=itemgetter(1))
    # logging.info([(key,max(map(itemgetter(0),ele))) for key, ele in tmp])
    # itemgetter actually getting the referenced value,
    # so to be used only once
    tbl_dts = [(key, max(map(itemgetter(0), ele))) for key, ele in tmp]
    # logging.info(tbl_dts)

    end_date = datetime.now().date()
    tbl_dts = [(table, start_date, end_date) for table, start_date in tbl_dts]

    # logging.info(tbl_dts)

    st_dt = dict(
        (table, start_date + timedelta(days=1))
        for table, start_date, end_date in tbl_dts
    )
    end_dt = dict((table, end_date) for table, start_date, end_date in tbl_dts)
    # logging.info(st_dt)
    # logging.info(end_dt)
    gc.collect()

    return st_dt, end_dt
