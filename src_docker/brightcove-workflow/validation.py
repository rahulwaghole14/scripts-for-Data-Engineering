"""
validators for piano subscriptions endpoint
"""
# pylint: disable=line-too-long

from datetime import date, datetime
import logging
from typing import Optional, List
from pydantic import ConfigDict, Field, validator
from common.validation.validators import MyBaseModel


class BrightcoveHourlyResponse(MyBaseModel):
    """BrightcoveHourlyResponse"""

    model_config = ConfigDict(extra="forbid")

    date_hour: datetime
    date: date
    video_view: int
    video_impression: int
    video_percent_viewed: float
    video_seconds_viewed: int
    brightcove_account: str
    record_load_dts: datetime


class BrightcoveDestinationPathResponse(MyBaseModel):
    """BrightcoveDestinationPathResponse"""

    model_config = ConfigDict(extra="forbid")

    date: date
    destination_path: Optional[str] = None
    video_view: int
    video_impression: int
    video_percent_viewed: float
    video_seconds_viewed: int
    ad_mode_begin: int
    ad_mode_complete: int
    brightcove_account: str
    record_load_dts: datetime


class BrightcoveResponse(MyBaseModel):
    """BrightcoveResponse"""

    model_config = ConfigDict(extra="forbid")

    video: Optional[str] = None
    video_reference_id: Optional[str] = Field(None, alias="video.reference_id")
    date: date
    video_created_at: Optional[datetime] = Field(
        None, alias="video.created_at"
    )
    video_updated_at: Optional[datetime] = Field(
        None, alias="video.updated_at"
    )
    name: Optional[str] = Field(None, alias="video_name")
    video_name: Optional[str] = Field(None, alias="video.name")
    video_tags: Optional[List[str]] = Field(None, alias="video.tags")
    ad_mode_begin: int
    video_impression: int
    video_state: Optional[str] = Field(None, alias="video.state")
    duration: Optional[int] = Field(None, alias="video_duration")
    video_duration: Optional[int] = Field(None, alias="video.duration")
    ad_mode_complete: int
    video_engagement_1: Optional[float] = None
    video_engagement_25: Optional[float] = None
    video_engagement_50: Optional[float] = None
    video_engagement_75: Optional[float] = None
    video_engagement_100: Optional[float] = None
    engagement_score: Optional[float] = None
    video_seconds_viewed: int
    video_percent_viewed: float
    video_play_rate: Optional[float] = None
    video_description: Optional[str] = Field(None, alias="video.description")
    video_long_description: Optional[str] = Field(
        None, alias="video.long_description"
    )
    video_complete: Optional[bool] = Field(None, alias="video.complete")
    video_economics: Optional[str] = Field(None, alias="video.economics")
    video_cq_asset_id: Optional[str] = Field(
        None, alias="video.custom_fields.cq_asset_id"
    )
    video_video_producer: Optional[str] = Field(
        None, alias="video.custom_fields.video_producer"
    )
    video_cast: Optional[str] = Field(None, alias="video.custom_fields.cast")
    video_source: Optional[str] = Field(
        None, alias="video.custom_fields.source"
    )
    video_video_source: Optional[str] = Field(
        None, alias="video.custom_fields.video_source"
    )
    video_provider: Optional[str] = Field(
        None, alias="video.custom_fields.provider"
    )
    video_mapped: Optional[str] = Field(
        None, alias="video.custom_fields.mapped"
    )
    video_ad_timecodes: Optional[str] = Field(
        None, alias="video.custom_fields.ad_timecodes"
    )
    video_consumer_advice: Optional[str] = Field(
        None, alias="video.custom_fields.consumer_advice"
    )
    video_aggregated_reference_ids: Optional[str] = Field(
        None, alias="video.custom_fields.aggregated_reference_ids"
    )
    video_accesslevel: Optional[str] = Field(
        None, alias="video.custom_fields.accesslevel"
    )
    video_live: Optional[str] = Field(None, alias="video.custom_fields.live")
    video_thumbnailtype: Optional[str] = Field(
        None, alias="video.custom_fields.thumbnailtype"
    )
    video_once_status: Optional[str] = Field(
        None, alias="video.custom_fields.once_status"
    )
    video_once_enabled: Optional[str] = Field(
        None, alias="video.custom_fields.once_enabled"
    )
    video_environment: Optional[str] = Field(
        None, alias="video.custom_fields.environment"
    )
    video_bc_meeting_id: Optional[str] = Field(
        None, alias="video.custom_fields.bc_meeting_id"
    )
    video_releasedate: Optional[datetime] = Field(
        None, alias="video.custom_fields.releasedate"
    )
    video_classification_rating: Optional[str] = Field(
        None, alias="video.custom_fields.classification_rating"
    )
    video_pos: Optional[str] = Field(None, alias="video.custom_fields.pos")
    video_tw_tweet: Optional[str] = Field(
        None, alias="video.custom_fields.tw_tweet"
    )
    video_parentalrating: Optional[str] = Field(
        None, alias="video.custom_fields.parentalrating"
    )
    video_videotype: Optional[str] = Field(
        None, alias="video.custom_fields.videotype"
    )
    video_director: Optional[str] = Field(
        None, alias="video.custom_fields.director"
    )
    video_hidetitlefromthumbnail: Optional[str] = Field(
        None, alias="video.custom_fields.hidetitlefromthumbnail"
    )
    video_once_data: Optional[str] = Field(
        None, alias="video.custom_fields.once_data"
    )
    video_genre: Optional[str] = Field(None, alias="video.custom_fields.genre")
    video_ovu_oovvuu_id: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_oovvuu_id"
    )
    video_ovu_production_year: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_production_year"
    )
    video_ovu_ad_timecodes: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_ad_timecodes"
    )
    video_ovu_provider: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_provider"
    )
    video_ovu_show_id: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_show_id"
    )
    video_ovu_genre: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_genre"
    )
    video_ovu_advisory: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_advisory"
    )
    video_ovu_show_name: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_show_name"
    )
    video_ovu_season: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_season"
    )
    video_ovu_episode: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_episode"
    )
    video_ovu_show_description: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_show_description"
    )
    video_ovu_credits_timecode: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_credits_timecode"
    )
    video_ovu_classification: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_classification"
    )
    video_ovu_short_title: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_short_title"
    )
    video_ovu_provider_details: Optional[str] = Field(
        None, alias="video.custom_fields.ovu_provider_details"
    )
    video_view: int
    brightcove_account: str
    record_load_dts: datetime

    # Custom validator for `video_releasedate`
    @validator("video_releasedate", pre=True)
    def parse_date(cls, value):  # pylint: disable=no-self-argument
        """parse_date"""
        if isinstance(value, str):
            try:
                # Handle year-only input by assuming a default date, e.g., January 1st of that year
                if value.isdigit() and len(value) == 4:
                    value += (
                        "/01/01"  # Default to January 1st of the given year
                    )
                return datetime.strptime(value, "%Y/%m/%d")
            except ValueError as e:
                logging.info("Error while parsing date: %s", str(e))
                return None
                # raise ValueError(f"Input should be a valid datetime, invalid date format: {value}") from e
        return value
