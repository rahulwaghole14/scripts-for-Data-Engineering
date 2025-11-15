"""Main module for the google_ad_manager__to_bigquery DAG."""
from datetime import datetime, timedelta
import os
import pytz

from google_ad_manager__to_bigquery.load_data import (
    load_data_for_date_range,
    load_data_for_date_range_validated,
)
from google_ad_manager__to_bigquery.load_config import load_config
from google_ad_manager__to_bigquery.PCAadwallet import main as run_pca_adwallet
from google_ad_manager__to_bigquery.validation import (
    PcaGen,
    PcaHourly,
    # PcaView,
    PcaViewVersion101,
    PcaViewVersion102,
    PcaViewVersion103,
    SponCon,
    # VideoViewership,
)

from a_common.logging.logger import logger, log_start, log_end

# Set up logging
logger(
    "google_ad_manager__to_bigquery",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


def run_generic(
    start_date_gen,
    end_date_gen,
    query_id,
    table_suffix,
    skip_deletion: bool = False,
    pydantic_data_model=None,
):  # pylint: disable=too-many-arguments
    """
    load data for the defined date range
    """

    # if pydanticDataModel is not none then use validated function else use normal function
    if pydantic_data_model:
        load_data_for_date_range_validated(
            query_id,
            start_date_gen,
            end_date_gen,
            table_suffix,
            skip_deletion,
            pydantic_data_model=pydantic_data_model,
        )
    else:
        load_data_for_date_range(
            query_id, start_date_gen, end_date_gen, table_suffix, skip_deletion
        )


def main():
    """Run workflows."""

    config = load_config()

    # Set New Zealand Auckland timezone
    nz_tz = pytz.timezone("Pacific/Auckland")
    three_days_ago = datetime.now(nz_tz) - timedelta(days=3)
    tomorrow = datetime.now(nz_tz) + timedelta(days=1)

    run_generic(
        three_days_ago,
        datetime.now(nz_tz) - timedelta(days=1),
        config["PCA_GEN_QUERY"],
        "pca_gen",
        False,
        PcaGen,
    )
    run_generic(
        datetime.now(nz_tz) - timedelta(days=5),
        datetime.now(nz_tz) - timedelta(days=1),
        config["FILL_RATE"],
        "fill_rate",
        True,
    )
    run_generic(
        three_days_ago,
        datetime.now(nz_tz) - timedelta(days=1),
        config["PCA_HOUR_QUERY"],
        "pca_hour",
        False,
        PcaHourly,
    )
    run_generic(
        # three_days_ago,
        datetime.now(nz_tz) - timedelta(days=3),
        datetime.now(nz_tz) - timedelta(days=1),
        config["PCA_VIEW_101_QUERY"],
        "pca_view",
        False,
        PcaViewVersion101,
    )
    run_generic(
        # three_days_ago,
        datetime.now(nz_tz) - timedelta(days=3),
        datetime.now(nz_tz) - timedelta(days=1),
        config["PCA_VIEW_102_QUERY"],
        "pca_view_video",
        False,
        PcaViewVersion102,
    )
    run_generic(
        # three_days_ago,
        # datetime.now(nz_tz) - timedelta(days=308), # 23 Nov 2023, 18:04:35 UTC+13 backfill
        datetime.now(nz_tz) - timedelta(days=3),
        datetime.now(nz_tz) - timedelta(days=1),
        config["PCA_VIEW_103_QUERY"],
        "pca_view_video_adwallet",
        False,
        PcaViewVersion103,
    )
    run_generic(
        three_days_ago,
        datetime.now(nz_tz) - timedelta(days=1),
        config["VV_GEN_QUERY"],
        "vv_gen",
        False,
    )
    run_generic(
        tomorrow,
        datetime.now(nz_tz) + timedelta(days=89),
        config["VV_SELLTHROUGH_QUERY"],
        "vv_sellthrough",
        False,
    )
    run_generic(
        three_days_ago,
        datetime.now(nz_tz) - timedelta(days=1),
        config["SPON_CON_QUERY"],
        "gam_sponcon",
        False,
        SponCon,
    )
    run_pca_adwallet()


if __name__ == "__main__":
    log_start()
    main()
    log_end()
