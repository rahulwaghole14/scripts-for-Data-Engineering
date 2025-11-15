import requests
from common.common import BASE_URL, REQUEST_HEADERS


def get_segment_name(segment_id):
    request_url = BASE_URL + 'segments/'
    response = requests.get(
                url=request_url + segment_id, headers=REQUEST_HEADERS)
    return response.json()['name']

def get_dimension_name(dimension_id):
    request_url = BASE_URL + 'dimensions/'
    response = requests.get(
                url=request_url + dimension_id, headers=REQUEST_HEADERS, params  = {"rsid": "fairfaxnzhexa-new-replatform"})
    return response.json()['name']


def get_metric_name(metric_id):
    request_url = BASE_URL + 'metrics/'
    response = requests.get(
                url=request_url + metric_id, headers=REQUEST_HEADERS, params  = {"rsid": "fairfaxnzhexa-new-replatform"})
    return response.json()['name']


def get_calculated_metric_name(calculated_metric_id):
    request_url = BASE_URL + 'calculatedmetrics/'
    response = requests.get(
                url=request_url + calculated_metric_id, headers=REQUEST_HEADERS)
    return response.json()['name']

def get_overall_metric_name(metric_id):
    output = "missing"
    if metric_id[0:2] == 'cm':
        output = get_calculated_metric_name(metric_id)
    elif "/" in metric_id:
        output = get_metric_name(metric_id.split("/")[1])
    return output 
