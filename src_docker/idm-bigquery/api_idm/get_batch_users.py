"""import requires"""
import logging
from .get_token_users import get_users


def batch_get_users(
    api_url,
    apiauth,
    count,
    start_id,
    end_id,
    start_datetime,
    end_datetime=None,
    mode=1,
):  # pylint: disable=too-many-arguments
    """get the users in batch"""
    start_index = 0

    # batch mode (range of dates)
    if mode == 0:
        filters = f'''filter=meta.lastModified ge "{start_datetime}" \
            and meta.lastModified le "{end_datetime}"'''
    # batch mode (from date)
    elif mode == 1:
        filters = f'''filter=meta.lastModified ge "{start_datetime}"'''
    elif mode == 2:
        filters = f'''filter=meta.lastModified ge "{start_datetime}" \
            and meta.lastModified le "{end_datetime}" \
            and id ge "{start_id}" \
            and id le "{end_id}"'''

    else:
        return []

    query_params = f"""?count={count}&startIndex={start_index}&{filters}"""

    response = get_users(api_url, query_params, apiauth)
    # log start_index & total_results & count & number of responses
    logging.info(
        "[]<>[] start index: %s total results: %s count: %s number of responses: %s",
        start_index,
        response["totalResults"],
        count,
        len(response["Resources"]),
    )

    responses = []
    total_results = None

    # Append first page of responses to list
    responses += response["Resources"]
    # print(len(responses))

    # Get total number of results from first page
    total_results = int(response["totalResults"])
    logging.info("total results to get: %s", total_results)

    prev_num_responses = 0

    # Make subsequent API requests to get remaining pages of responses
    while len(responses) < total_results:
        start_index += count
        query_params = f"""?count={count}&startIndex={start_index}&{filters}"""
        response = get_users(api_url, query_params, apiauth)
        logging.info(
            "[[api response]] start index: %s total results: %s count: %s number of responses: %s",
            start_index,
            response["totalResults"],
            count,
            len(response["Resources"]),
        )
        responses += response["Resources"]
        # logging.info(len(responses))

        # Condition to stop: If the number of responses hasn't changed, return the responses
        if len(responses) == prev_num_responses:
            logging.info(
                "Number of responses hasn't changed. Returning responses."
            )
            return responses

        prev_num_responses = len(responses)

    return responses
