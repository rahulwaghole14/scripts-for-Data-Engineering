# pylint: disable=all

from datetime import datetime, timedelta, date
import logging

# big query
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# adobe analytics
from .common.reports.report_query import initialise_query
from .common.reports.report_query import get_report_query
from .common.reports.report_query import get_report_query_limit
from .common.reports.report_query import (
    initialise_query_date,
    initialise_query_between_dates,
)
from .common.reports.query import Metric


from .bigquery_module import *
from .logging_module import logger

logger("extract_data_fix")

import pandas as pd


def process_request(df, metric_col_names, value_col_name, drop_itemID):
    """
    Process dataframe responsed by Adobe API 2.0.

    :param df: The original dataframe.
    :type df: Pandas dataframe.

    :returns: The processed dataframe.
    :rtype: Pandas dataframe.
    """
    try:
        df[metric_col_names] = pd.DataFrame(df.data.values.tolist())
        if drop_itemID == 1:
            df = df.drop(["data", "itemId"], axis=1)
        df.rename(columns={"value": value_col_name}, inplace=True)
        logging.info(df)

        return df
    except Exception as error:
        logging.info("Processing request failed in df")
        logging.info("Processing request failed in df: %s", str(error))
        return None


def extract_data_for_mobile_device(table_id, client, start_date, end_date):

    try:

        bq_schema = [
            bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField(
                "ContentID", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Mobile_device", bigquery.enums.SqlTypeNames.STRING
            ),
            bigquery.SchemaField(
                "Page_Views", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Unique_Visitors", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Average_time_spent", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Time_spent_per_visitor", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Time_spent_per_visit", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Load_datetime", bigquery.enums.SqlTypeNames.DATETIME
            ),
        ]

        logging.info("Creating Query for Mobile Device Data")
        # s200000657_55d27bbae4b04cb8a91e784f -> Countries = NZ
        segment_id = ["s200000657_5db8a82471ac0108a6900555"]

        metric1 = Metric("metrics/pageviews", 0)
        metric2 = Metric("metrics/visitors", 1)
        metric3 = Metric("cm_average_time_on_site_defaultmetric", 2)
        metric4 = Metric("metrics/timespentvisitor", 3)
        metric5 = Metric("metrics/timespentvisit", 4)
        # metric6 = Metric('metrics/bouncerate', 5)

        metrics = [metric1, metric2, metric3, metric4, metric5]
        metric_col_names = [
            "Page_Views",
            "Unique_Visitors",
            "Average_time_spent",
            "Time_spent_per_visitor",
            "Time_spent_per_visit",
        ]

        dimension = "variables/mobiledevicetype"
        breakdown_dimension = {}

        logging.info("Initializing queries for main dimension")
        start_date = date(
            2023, 12, 1
        )  # change dates based on required backfill dates
        end_date = date(2023, 12, 2)
        # get the list of missing dates or number of days
        nod = (end_date - start_date).days
        logging.info("number of days")
        logging.info(nod)
        fix_merge = pd.DataFrame()
        logging.info("logging.info fix merge")
        logging.info(fix_merge)

        query = (
            "SELECT Date, ContentID, Mobile_device, Page_views, Unique_Visitors, \
                    Average_time_spent,Time_spent_per_visitor, Time_spent_per_visit \
                    FROM `hexa-data-report-etl-prod.sponsored_content.mobile_device` \
                where Date between '"
            + str(start_date)
            + "' and '"
            + str(end_date)
            + "'"
        )

        data = client.query(query).result().to_dataframe()
        logging.info("logging.infoing the data")
        logging.info(data)

        for n in range(nod):
            logging.info("end_date")
            logging.info(end_date)  # end_date = start_date + timedelta(days=1)
            merge = pd.DataFrame()
            logging.info("merged data")
            logging.info(merge)
            logging.info(
                "{}, start date = {}, end date = {}".format(
                    n + 1, start_date, end_date
                )
            )

            dimension = "variables/mobiledevicetype"
            breakdown_dimension = {}

            initialise_query_date(
                dimension=dimension,
                breakdown_dimension=breakdown_dimension,
                segment_id=segment_id,
                dates=start_date,
                metrics=metrics,
            )

            df_itemID = get_report_query_limit()
            logging.info("df_itemID")
            logging.info(df_itemID)

            if not df_itemID.empty:
                df_itemID = process_request(
                    df_itemID, metric_col_names, "Mobile_device", 0
                )
                logging.info(
                    "{0} Mobile devices found for the segment_id {1}".format(
                        df_itemID.shape[0], segment_id
                    )
                )

                for index, row in df_itemID.iterrows():
                    dimension = "variables/prop11"
                    breakdown_dimension = {
                        "type": "breakdown",
                        "dimension": "variables/mobiledevicetype",
                        "itemId": str(row["itemId"]),
                    }
                    logging.info("inside for loop")

                    initialise_query_date(
                        dimension=dimension,
                        breakdown_dimension=breakdown_dimension,
                        segment_id=segment_id,
                        dates=start_date,
                        metrics=metrics,
                    )
                    logging.info("brekdown query")
                    logging.info(query)

                    df = get_report_query_limit()

                    if not df.empty:
                        df = process_request(
                            df, metric_col_names, "ContentID", 1
                        )
                        df["Mobile_device"] = row["Mobile_device"]
                        df["Date"] = start_date
                        # merge = merge.append(df)
                        merge = pd.concat([merge, df], ignore_index=False)
                        # logging.info(df.columns)
                        logging.info("DataFrame columns: %s", list(df.columns))

            if not merge.empty:
                merge.dropna(subset=["ContentID"], inplace=True)
                merge["Load_datetime"] = datetime.now()
                columns_to_int = ["ContentID", "Page_Views", "Unique_Visitors"]
                merge[columns_to_int] = merge[columns_to_int].astype("int64")

                columns_order = [
                    "Date",
                    "ContentID",
                    "Mobile_device",
                    "Page_Views",
                    "Unique_Visitors",
                    "Average_time_spent",
                    "Time_spent_per_visitor",
                    "Time_spent_per_visit",
                ]
                merge = merge[columns_order]
                logging.info(merge)

                # fix_merge = fix_merge.append(merge)
                fix_merge = pd.concat([fix_merge, merge], ignore_index=False)
                logging.info("fix merge is")
                logging.info("rename columns")
                data.rename(columns={"Page_views": "Page_Views"}, inplace=True)
                # logging.info("fix_merge columns:", fix_merge.columns)
                # logging.info("data columns:", data.columns)
                logging.info("fix_merge columns: %s", list(fix_merge.columns))
                logging.info("data columns: %s", list(data.columns))

        # outer_join = fix_merge.merge(data, how='outer', indicator=True)
        # columns_keep = ['Date', 'ContentID', 'Mobile_device', 'Page_views', 'Unique_Visitors',
        #                         'Average_time_spent', 'Time_spent_per_visitor', 'Time_spent_per_visit']
        outer_join = fix_merge.merge(
            data, how="outer", on=columns_order, indicator=True
        )

        # logging.info("Merged columns:", outer_join.columns)
        logging.info("Merged columns: %s", outer_join.columns)

        mask = outer_join["_merge"] == "left_only"
        anti_join = outer_join.loc[mask]

        # anti_join = outer_join[~(outer_join.merge == 'both')].drop('_merge', axis=1)

        anti_join = anti_join[columns_order]
        columns_to_int = ["ContentID", "Page_Views", "Unique_Visitors"]
        anti_join[columns_to_int] = anti_join[columns_to_int].astype("int64")
        anti_join["Load_datetime"] = datetime.now()

        logging.info(
            "{} contentids were missing".format(anti_join.ContentID.nunique())
        )
        append_dataframe_to_bigquery(
            anti_join, client, table_id, bq_schema, start_date
        )

    # anti_join.to_csv(os.getcwd()+"\\mobile_device_missing_data_"+str(start_date)+".csv", index=False)

    except Exception as e:
        logging.error("An error occurred in extract_data_fix: %s", str(e))
