'''import requires'''
import logging
from .get_token_users import get_users

def batch_get_users(api_url,username,password,apiauth,count,start_id, end_id, start_datetime,end_datetime=None, mode=1 ):
    ''' get the users in batch'''
    start_index = 0

    # batch mode (range of dates)
    if mode == 0 :
        filters = f'''filter=meta.lastModified ge "{start_datetime}" \
            and meta.lastModified le "{end_datetime}"'''
    # batch mode (from date)
    elif mode == 1 :
        filters = f'''filter=meta.lastModified ge "{start_datetime}"'''
    elif mode == 2 :
        filters = f'''filter=meta.lastModified ge "{start_datetime}" \
            and meta.lastModified le "{end_datetime}" \
            and id ge "{start_id}" \
            and id le "{end_id}"'''

    else :
        return []

    query_params = f'''?count={count}&startIndex={start_index}&{filters}'''

    response = get_users(api_url,query_params,username,password,apiauth)
    # log start_index & total_results & count & number of responses
    logging.info("[]<>[] start index: %s total results: %s count: %s number of responses: %s"
                 , start_index, response['totalResults'], count, len(response['Resources'])
                 )

    responses = []
    total_results = None

    # Append first page of responses to list
    responses += response['Resources']
    # print(len(responses))

    # Get total number of results from first page
    total_results = int(response['totalResults'])
    logging.info("total results to get: %s", total_results)

    prev_num_responses = 0

    # Make subsequent API requests to get remaining pages of responses
    while len(responses) < total_results:
        # logging.info("getting next batch: index: " + str(start_index) + " records: " + str(len(responses)))
        start_index += count
        query_params = f'''?count={count}&startIndex={start_index}&{filters}'''
        response = get_users(api_url,query_params,username,password,apiauth)
        logging.info("[[api response]] start index: %s total results: %s count: %s number of responses: %s",
                     start_index,
                     response['totalResults'],
                     count,
                     len(response['Resources'])
                )
        responses += response['Resources']
        # logging.info(len(responses))

        # Condition to stop: If the number of responses hasn't changed, return the responses
        if len(responses) == prev_num_responses:
            logging.info("Number of responses hasn't changed. Returning responses.")
            return responses

        prev_num_responses = len(responses)

    return responses

def remove_duplicates(bad_data):
    ''' remove the duplicates'''
    # handle empty list
    if not bad_data:
        return []

    # handle data is None
    if bad_data is None:
        return []

    # Remove any records that don't have an "id" key
    user_records = [record for record in bad_data if "id" in record]
    grouped_records = []
    for record in user_records:
        user_id = record["id"]
        matching_records = [r for r in grouped_records if r["id"] == user_id]
        if matching_records:
            latest_record = max(matching_records, key=lambda r: r["meta"]["lastModified"])
            if record["meta"]["lastModified"] > latest_record["meta"]["lastModified"]:
                grouped_records.remove(latest_record)
                grouped_records.append(record)
        else:
            grouped_records.append(record)
    # return grouped and sorted list
    return sorted(grouped_records, key=lambda r: r["id"])
