"""
Breakdown solution
Query initialisation can now can take primary dimension, breakdown dimensions, segment id, metrics, dates,& no. of days.
"""

# pylint: disable=import-error
import json
import datetime
import logging
import pandas as pd
from datetime import datetime, time, timedelta
import pandas
import requests
from common.reports.query import Query
from dateparser import parse
# Global query variable
query = None


LIMIT = 100000

def initialise_query(
    report_suite_id,
    dimension,
    breakdown_dimension,
    segment_id,
    metrics,
    no_of_days,
):
    global query
    today = datetime.date.today()
    time_delta = datetime.timedelta(days=no_of_days)
    historical = today - time_delta
    datetime_resolution = datetime.time(0, 0, 0, microsecond=0)
    historical_today = historical + datetime.timedelta(days=no_of_days)

    historical_today = str(historical_today) + "T" + str(datetime_resolution)
    logging.info("Historical today = %s", historical_today)
    historical = str(historical) + "T" + str(datetime_resolution)
    logging.info("Historical yesterday= %s", historical)

    if breakdown_dimension:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .metrics_containers_filters(breakdown_dimension)
            .limit(LIMIT)
        )
    else:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .limit(LIMIT)
        )

    return None

def initialise_query_between_dates(
    report_suite_id,
    dimension,
    breakdown_dimension,
    segment_id,
    metrics,
    start_date,
    end_date,
):
    global query
    datetime_resolution = datetime.time(0, 0, 0, microsecond=0)

    historical_today = str(end_date) + "T" + str(datetime_resolution)
    logging.info("Historical today = %s", historical_today)
    historical = str(start_date) + "T" + str(datetime_resolution)
    logging.info("Historical yesterday= %s", historical)

    if breakdown_dimension:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .metrics_containers_filters(breakdown_dimension)
            .limit(LIMIT)
        )
    else:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .limit(LIMIT)
        )

    return None

def initialise_query_date(report_suite_id, dimension, breakdown_dimension, segment_id, start_date, metrics, no_of_days=1):
    global query
    
    historical = datetime.strptime(start_date, "%Y-%m-%d").date()
    datetime_resolution = time(0, 0, 0, microsecond=0)
    historical_today = historical + timedelta(days=no_of_days)

    historical_today = str(historical_today) + "T" + str(datetime_resolution)
    historical = str(historical) + "T" + str(datetime_resolution)

    if breakdown_dimension:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .metrics_containers_filters(breakdown_dimension)
            .limit(LIMIT)
        )
    else:
        query = (
            Query(report_suite_id)
            .select(dimension, metrics)
            .for_range(parse(historical), parse(historical_today))
            .with_segments(segment_id)
            .limit(LIMIT)
        )

    return query


# Modify get_report_query and get_report_query_limit to accept query as a parameter:

def get_report_query(base_url, headers, query):
    df = pd.DataFrame()
    while True:
        payload = json.dumps(query.compile())
        response = requests.post(
            f"{base_url}/reports", 
            headers=headers, 
            data=payload, 
            timeout=300
        )
        response.raise_for_status()
        json_response = response.json()
        
        page_df = pd.DataFrame(json_response.get('rows', []))
        df = pd.concat([df, page_df], ignore_index=True)
        
        if json_response.get('lastPage', True):
            break
        
        query = query.next()
    
    return df

def get_report_query_limit(base_url, headers, query):
    url = f"{base_url}/reports"
    params = json.dumps(query.compile())
    response = requests.post(url, headers=headers, data=params, timeout=300)
    response.raise_for_status()
    json_response = response.json()
    
    return pd.DataFrame(json_response.get('rows', []))

def get_report_query_with_pagination(base_url, headers, query):
    df = pd.DataFrame()
    while True:
        response = requests.post(f"{base_url}/reports", headers=headers, data=json.dumps(query.compile()), timeout=300)
        response.raise_for_status()
        json_response = response.json()
        
        page_df = pd.DataFrame(json_response.get('rows', []))
        df = pd.concat([df, page_df], ignore_index=True)
        
        if json_response.get('lastPage', True):
            break
        
        query = query.next()
    
    return df
