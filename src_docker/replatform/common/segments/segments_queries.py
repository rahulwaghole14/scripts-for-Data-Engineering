import requests
import json
from common.common import BASE_URL,REQUEST_HEADERS


def segment_info(segment, request_body={'expansion':['definition','organization']} ):

    request_url = BASE_URL + 'segments/' + segment

    response = requests.get(url=request_url, headers=REQUEST_HEADERS, params=request_body)

    json_response = response.json()

    return json_response

def update_segment_defintion(segment, update_definition):

    url = BASE_URL + 'segments/' + segment + '?locale=en_US&expansion=definition'
    segment_data = json.dumps({"id": segment, "name": "name", "description": "description",
                "rsid": 'fairfaxnzhexa-new-replatform', "owner": {"id": "200254191"}, "definition": update_definition})

    
    response = requests.put(url=url, headers=REQUEST_HEADERS, data=segment_data)
    if response.status_code != 200:
        print("error updating segment")
        print(update_definition)
        print(response)
        print(response.text)
        print(response.json)
        print("aaa")
        quit()
    return response

