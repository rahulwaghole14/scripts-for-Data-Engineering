import sys
import requests
import pandas as pd
import logging
from common.common import BASE_URL, REQUEST_HEADERS
from common.reports.report_columns import get_query_column_names
from common.reports.map_fields import get_dimension_name
from json import dumps
from common.oberon_queries.oberon_functions import update_query_pagination

"""
Segments:
's200000657_566f38e9e4b080432935e591': Domestic Traffic NZ
's200000657_5acd2e79e16ac06b09e0138b': ??
's200000657_5db8a82471ac0108a6900555': sponsored_content
's200000657_562858f4e4b0c155a4d7244f': facebook referall

Metrics:
metrics/visitors
metrics/pageviews
"cm200000657_5b15fac93916f64a32ab5eaf": PV/AV
"""


def get_report_query(request_body):
    request_url = BASE_URL + 'reports/'
    json_response = {}
    try:
        response = requests.post(
            url=request_url, headers=REQUEST_HEADERS, data=request_body)
        response.raise_for_status()
        json_response = response.json()
        df = pd.DataFrame.from_dict(json_response['rows'], orient='columns')
        logging.info('Report Query retrieved {0} rows.'.format(df.shape[0]))
    except requests.exceptions.RequestException as err:
        logging.info(err.response.text)
        quit()
    finally:
        logging.info('Report Query Status Code = {0}.'.format(
            response.status_code))
    return df, json_response, response


def process_report(query):
    df = pd.DataFrame()
    pagination = 0
    while True:
        #json_request = dumps(query.compile())
        update_query_pagination(query, pagination)
        json_request = dumps(query)
        page_df, json_response, response = get_report_query(json_request)
        isLastPage = json_response['lastPage']
        #df = df.append(page_df, ignore_index=True)
        df = pd.concat([df, page_df])
        if pagination == 0:
            col_names = get_query_column_names(query)
            dim_name = get_dimension_name(query['dimension'].split("/")[1])
        pagination = pagination + 1
        if isLastPage:
            break
    df = df.rename(columns={"value": dim_name})
    df[col_names] = df.data.values.tolist()
    df = df.drop(['itemId','data'], axis=1)
    df = df.fillna(0)
    
    #df['Bounce Rate'] = df['Bounce Rate'].fillna(0)

    return df




