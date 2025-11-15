""" get data from adobe """
# pylint: disable=import-error

import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from common.reports.query import Metric
from common.reports.report_query import (
    get_report_query,
    get_report_query_limit,
    initialise_query_date,
    get_report_query_with_pagination,
)

def process_request(data_frame, metric_col_names, value_col_name, drop_itemid):
    """
    Process dataframe responsed by Adobe API 2.0.

    :param data_frame: The original dataframe.
    :type data_frame: Pandas dataframe.

    :returns: The processed dataframe.
    :rtype: Pandas dataframe.
    """
    try:
        data_frame[metric_col_names] = pd.DataFrame(
            data_frame.data.values.tolist()
        )
        if drop_itemid == 1:
            data_frame = data_frame.drop(["data", "itemId"], axis=1)
        data_frame.rename(columns={"value": value_col_name}, inplace=True)
        return data_frame
    except Exception as error:
        logging.info("Processing request failed %s", error)
        return None



def extract_data_for_engagement_table(start_date, headers):
    """extract data for eng table with improved performance"""
    COMPANY_ID = 'fairfa5'
    base_url = 'https://analytics.adobe.io/api/' + COMPANY_ID 
    report_suite_id = "fairfaxnz-hexaoverall-production"

    logging.info(f"Creating Query for Rego data table for date: {start_date}")
    segment_id = []

    metrics = [
        Metric("metrics/event4", 0),  # registration submitted
        Metric("metrics/event3", 1),  # Registration Start
        Metric("metrics/event13", 2),  # Login Start
        Metric("metrics/event73", 3),  # Link Click
    ]
    metric_col_names = [
        "registration_submitted",
        "registration_start",
        "login_start",
        "link_click",
    ]

    # Initial query
    dimension = "variables/evar143"
    dimension_name = "link_name"
    
    logging.info(f"Initializing initial query for date: {start_date}")
    query = initialise_query_date(
        report_suite_id=report_suite_id,
        dimension=dimension,
        breakdown_dimension={},
        segment_id=segment_id,
        start_date=start_date,
        metrics=metrics,
    )

    logging.info("Fetching initial data")
    df_itemid = get_report_query_limit(base_url, headers, query)

    if df_itemid.empty:
        logging.info("No data found for the initial query.")
        return pd.DataFrame()

    logging.info("Processing initial data")
    df_itemid = process_request(df_itemid, metric_col_names, dimension_name, 0)

    total_items = len(df_itemid)
    logging.info(f"Total items to process: {total_items}")

    # Batch processing
    batch_size = 200  # Increased batch size
    
    def process_batch(batch, batch_num, total_batches):
        logging.info(f"Processing batch {batch_num}/{total_batches}")
        breakdown_query = initialise_query_date(
            report_suite_id=report_suite_id,
            dimension="variables/evar13",
            breakdown_dimension={
                "type": "breakdown",
                "dimension": "variables/evar143",
                "itemIds": batch['itemId'].tolist()
            },
            segment_id=segment_id,
            start_date=start_date,
            metrics=metrics,
        )
        df_break = get_report_query_with_pagination(base_url, headers, breakdown_query)
        
        if not df_break.empty:
            df_break = process_request(df_break, metric_col_names, "page_name_url", 1)
            df_break["link_name"] = batch["link_name"].iloc[0]  # Assuming all items in batch have same link_name
        
        logging.info(f"Completed processing batch {batch_num}/{total_batches}")
        return df_break

    merge = pd.DataFrame()
    total_batches = (total_items + batch_size - 1) // batch_size
    process_batch_partial = partial(process_batch, total_batches=total_batches)

    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        futures = []
        for i in range(0, total_items, batch_size):
            batch = df_itemid.iloc[i:i+batch_size]
            batch_num = i // batch_size + 1
            futures.append(executor.submit(process_batch_partial, batch, batch_num))
        
        for i, future in enumerate(as_completed(futures), 1):
            merge = pd.concat([merge, future.result()], ignore_index=True)
            logging.info(f"Completed batch {i}/{total_batches}")

    logging.info(f"Data extraction completed for date: {start_date}. Processed {total_items} items.")
    return merge