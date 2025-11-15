"""
Query initialisation can now can take primary dimension, breakdown dimensions, segment id, metrics, dates, & no. of days.
"""
# pylint: disable=all

import logging

# from datetime import timedelta, datetime, date
from json import dumps
import requests
import pandas
from dateparser import parse
from ..common import (
    BASE_URL,
    REQUEST_HEADERS,
)  # outh credentail tokens being set
from .query import Metric, Query
from .datetime_handler import *
import datetime
from ...logging_module import logger
import pandas as pd

logger()


def initialise_query(
    dimension, breakdown_dimension, segment_id, metrics, no_of_day=1
):
    global query
    today = datetime.date.today()
    time_delta = datetime.timedelta(days=no_of_day)
    historical = today - time_delta
    datetime_resolution = datetime.time(0, 0, 0, microsecond=0)
    historical_today = historical + datetime.timedelta(days=no_of_day)

    historical_today = str(historical_today) + "T" + str(datetime_resolution)
    logging.info("Historical today = {}".format(historical_today))
    historical = str(historical) + "T" + str(datetime_resolution)
    logging.info("Historical yesterday= {}".format(historical))

    if breakdown_dimension:
        query = (
            Query("fairfaxnz-hexaoverall-production")
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .metrics_containers_filters(breakdown_dimension)
            .limit(50000)
        )
    else:
        query = (
            Query("fairfaxnz-hexaoverall-production")
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .limit(50000)
        )

    logging.info(query.compile())

    return query


def initialise_query_between_dates(
    dimension, breakdown_dimension, segment_id, metrics, start_date, end_date
):
    global query
    datetime_resolution = datetime.time(0, 0, 0, microsecond=0)

    historical_today = str(end_date) + "T" + str(datetime_resolution)
    logging.info("Historical today = {}".format(historical_today))
    historical = str(start_date) + "T" + str(datetime_resolution)
    logging.info("Historical yesterday= {}".format(historical))

    if breakdown_dimension:
        query = (
            Query("fairfaxnz-hexaoverall-production")
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .metrics_containers_filters(breakdown_dimension)
            .limit(50000)
        )
        logging.info("query for brekdown" + Query)
    else:
        query = (
            Query("fairfaxnz-hexaoverall-production")
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .limit(50000)
        )

    # logging.info(query.compile())

    return None


def initialise_query_date(
    dimension, breakdown_dimension, segment_id, metrics, dates
):
    """
    dates need to be in a format yyyy-mm-dd in date format
    nod = no of days from the dates; by default it is 1
    """

    try:

        global query
        no_of_day = 1

        historical = dates
        datetime_resolution = datetime.time(0, 0, 0, microsecond=0)
        historical_today = historical + datetime.timedelta(days=no_of_day)

        historical_today = (
            str(historical_today) + "T" + str(datetime_resolution)
        )
        logging.info("historical today = {}".format(historical_today))
        logging.info("funtion report")
        historical = str(historical) + "T" + str(datetime_resolution)
        logging.info("historical yesterday= {}".format(historical))

        if breakdown_dimension:
            query = (
                Query("fairfaxnz-hexaoverall-production")
                .select(dimension, metrics)
                .for_range(parse(historical), parse(historical_today))
                .with_segments(segment_id)
                .metrics_containers_filters(breakdown_dimension)
                .limit(50000)
            )
            logging.info("breakdown query")
            # logging.info(query)
        else:
            query = (
                Query("fairfaxnz-hexaoverall-production")
                .select(dimension, metrics)
                .for_range(parse(historical), parse(historical_today))
                .with_segments(segment_id)
                .limit(50000)
            )
            logging.info("BREKDOWN")
            # logging.info(query)
    except Exception as error:
        logging.info("error in function intilise_query %s", error)

    # logging.info(query.compile())

    return None


def get_report_query_page(request_body):

    logging.info(REQUEST_HEADERS)
    request_url = BASE_URL + "reports/"
    json_response = {}
    try:
        response = requests.post(
            url=request_url, headers=REQUEST_HEADERS, data=request_body
        )
        response.raise_for_status()
        json_response = response.json()
        df = pandas.DataFrame.from_dict(
            json_response["rows"], orient="columns"
        )
        # logging.info('Report Query retrieved {0} rows.'.format(df.shape[0]))
    except requests.exceptions.RequestException as err:
        logging.info(
            "error in function get_report_query_page %s", err.response.text
        )
        quit()
    finally:
        logging.info(
            "Report Query Status Code = {0}.".format(response.status_code)
        )

    return df, json_response, response


def get_report_query():
    df = pandas.DataFrame()
    while True:
        json_request = dumps(query.compile())
        page_df, json_response, response = get_report_query_page(json_request)
        isLastPage = json_response["lastPage"]
        # df = df.append(page_df, ignore_index=True)
        df = pd.concat([df, page_df], ignore_index=True)
        query.next()

        if isLastPage:
            break
    return df


def get_report_query_limit():  #  equivalent of getting the top N articles on 2020/03/04
    try:
        # df = pandas.DataFrame()
        df = pd.DataFrame()
        json_request = dumps(query.compile())
        page_df, json_response, response = get_report_query_page(json_request)
        # df = df.append(page_df, ignore_index=True)
        df = pd.concat([df, page_df], ignore_index=True)
        logging.info("df report limit")
        logging.info(df)
    except Exception as error:
        logging.info("error in get_report_query_limit %s", error)
    logging.info(df)

    return df
