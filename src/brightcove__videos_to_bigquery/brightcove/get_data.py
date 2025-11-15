""" get data from Brightcove API using the token """
import json
import logging
import os
from typing import List, Tuple, Union

import requests
from dotenv import load_dotenv

load_dotenv()

BRIGHTCOVE_DATA_URL = os.environ.get("BRIGHTCOVE_DATA_URL")


def build_url(  # pylint: disable=too-many-arguments
    base_url: str,
    account_id: str,
    dimensions: List[str],
    fields: List[str],
    date: str,
    limit: int,
    offset: int,
) -> str:
    """build url for Brightcove API request"""
    try:
        return (
            f"{base_url}?accounts={account_id}"
            f"&dimensions={','.join(dimensions)}"
            f"&fields={','.join(fields)}"
            f"&from={date}&to={date}"
            f"&limit={limit}&offset={offset}"
        )
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error building URL: %s", e)
        return None


def get_data_from_api(
    url: str, token: str, timeout: int = 120
) -> Union[None, Tuple[int, List]]:
    """get data from Brightcove API using the token"""
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url=url, headers=headers, timeout=timeout)

        if response.status_code != 200:
            logging.error(
                "API request failed with status %s: %s",
                response.status_code,
                response.text,
            )
            return None

        return (
            json.loads(response.text)["item_count"],
            json.loads(response.text)["items"],
        )

    except requests.exceptions.Timeout:
        logging.error("API request timed out.")
        return None
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error in API request: %s", e)
        return None


def get_daily_data(  # pylint: disable=too-many-arguments
    token: dict,
    date: str,
    brightcove_account_id: str,
    account_name: str,
    limit: int = 10,
    offset: int = 0,
) -> Tuple[int, List]:
    """
    Get daily data from Brightcove API
    """

    try:
        dimensions = ["date", "video"]
        fields = [
            "video",
            "video_duration",
            "video_engagement_1",
            "video_engagement_100",
            "video_engagement_25",
            "video_engagement_50",
            "video_engagement_75",
            "video_view",
            "video_impression",
            "video.name",
            "video_percent_viewed",
            "video_seconds_viewed",
            "video_play_rate:video_impression/video_view",
            "video_name",
            "ad_mode_begin",
            "ad_mode_complete",
            "date",
            "engagement_score",
            "video.reference_id",
            "video.description",
            "video.complete",
            "video.created_at",
            "video.duration",
            "video.economics",
            "video.long_description",
            "video.state",
            "video.tags",
            "video.updated_at",
            "video.custom_fields.source",
            "video.custom_fields.provider",
            "video.custom_fields.video_source",
            "video.custom_fields.video_producer",
            "video.custom_fields.cast",
            "video.custom_fields.director",
            "video.custom_fields.once_data",
            "video.custom_fields.once_enabled",
            "video.custom_fields.once_status",
            "video.custom_fields.accesslevel",
            "video.custom_fields.hidetitlefromthumbnail",
            "video.custom_fields.thumbnailtype",
            "video.custom_fields.releasedate",
            "video.custom_fields.parentalrating",
            "video.custom_fields.videotype",
            "video.custom_fields.live",
            "video.custom_fields.classification_rating",
            "video.custom_fields.consumer_advice",
            "video.custom_fields.ovu_short_title",
            "video.custom_fields.ovu_oovvuu_id",
            "video.custom_fields.ovu_provider_details",
            "video.custom_fields.ovu_production_year",
            "video.custom_fields.ovu_show_id",
            "video.custom_fields.ovu_show_description",
            "video.custom_fields.ovu_season",
            "video.custom_fields.ovu_episode",
            "video.custom_fields.ovu_genre",
            "video.custom_fields.ovu_ad_timecodes",
            "video.custom_fields.ovu_credits_timecode",
            "video.custom_fields.ovu_advisory",
            "video.custom_fields.genre",
            "video.custom_fields.ovu_provider",
            "video.custom_fields.environment",
            "video.custom_fields.pos",
            "video.custom_fields.ad_timecodes",
            "video.custom_fields.ovu_show_name",
            "video.custom_fields.ovu_classification",
            "video.custom_fields.mapped",
            "video.custom_fields.cq_asset_id",
            "video.custom_fields.aggregated_reference_ids",
            "video.custom_fields.tw_tweet",
            "video.custom_fields.bc_meeting_id",
        ]

        url = build_url(
            BRIGHTCOVE_DATA_URL,
            brightcove_account_id,
            dimensions,
            fields,
            date,
            limit,
            offset,
        )

        if not url:
            return 0, []

        token_value = token.get("access_token", "")
        total, items = get_data_from_api(url, token_value)

        if total is None or items is None:
            return 0, []

        logging.info(
            "[%s: %s] Got data from API, offset: %s, total: %s",
            brightcove_account_id,
            account_name,
            offset,
            total,
        )

        return total, items

    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error in get_daily_data: %s", e)
        return 0, []
