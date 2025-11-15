"""
validators for piano subscriptions endpoint
"""
# pylint: disable=line-too-long

from datetime import date, datetime
from typing import Optional
from pydantic import ConfigDict, Field, validator
from common.validation.validators import MyBaseModel


class PcaViewVersion103(MyBaseModel):
    """PcaView"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    CREATIVE_NAME: Optional[str] = Field(None, alias="Dimension.CREATIVE_NAME")
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    CREATIVE_ID: Optional[int] = Field(None, alias="Dimension.CREATIVE_ID")
    AD_TYPE_NAME: Optional[str] = Field(None, alias="Dimension.AD_TYPE_NAME")
    VIDEO_AD_TYPE_NAME: Optional[str] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_NAME"
    )
    AD_TYPE_ID: Optional[int] = Field(None, alias="Dimension.AD_TYPE_ID")
    VIDEO_AD_TYPE_ID: Optional[int] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_ID"
    )
    SALESPERSON_ID: Optional[int] = Field(
        None, alias="Dimension.SALESPERSON_ID"
    )
    SALESPERSON_NAME: Optional[str] = Field(
        None, alias="Dimension.SALESPERSON_NAME"
    )
    LINE_ITEM_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    LINE_ITEM_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    ORDER_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE: Optional[float] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )
    VIDEO_VIEWERSHIP_START: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_START"
    )
    VIDEO_VIEWERSHIP_FIRST_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_FIRST_QUARTILE"
    )
    VIDEO_VIEWERSHIP_MIDPOINT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_MIDPOINT"
    )
    VIDEO_VIEWERSHIP_THIRD_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_THIRD_QUARTILE"
    )
    VIDEO_VIEWERSHIP_COMPLETE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME"
    )
    VIDEO_VIEWERSHIP_COMPLETION_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETION_RATE"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE"
    )
    VIDEO_TRUEVIEW_SKIP_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_SKIP_RATE"
    )
    VIDEO_TRUEVIEW_VIEWS: Optional[int] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VIEWS"
    )
    VIDEO_TRUEVIEW_VTR: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VTR"
    )
    VIDEO_VIEWERSHIP_VIDEO_LENGTH: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIDEO_LENGTH"
    )
    VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN"
    )
    VIDEO_VIEWERSHIP_ENGAGED_VIEW: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_ENGAGED_VIEW"
    )
    VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE"
    )
    VIDEO_VIEWERSHIP_AUTO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AUTO_PLAYS"
    )
    VIDEO_VIEWERSHIP_CLICK_TO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_CLICK_TO_PLAYS"
    )

    @validator(
        "LINE_ITEM_END_DATE_TIME",
        "LINE_ITEM_START_DATE_TIME",
        "ORDER_START_DATE_TIME",
        "ORDER_END_DATE_TIME",
        pre=True,
        always=True,
    )
    def parse_datetime(cls, value):  # pylint: disable=no-self-argument
        """Parse datetime values"""
        if value == "-" or value == "Unlimited" or not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Invalid datetime value: {value}") from e

    @validator("*", pre=True, always=True)
    def handle_dash_as_null(cls, value):  # pylint: disable=no-self-argument
        """Replace '-' with None"""
        if value == "-":
            return None
        return value

    @validator("VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE", pre=True, always=True)
    def parse_percentage(cls, value):  # pylint: disable=no-self-argument
        """Parse percentage values"""
        if value is None:
            return value
        if isinstance(value, str) and value.endswith("%"):
            try:
                # Remove the '%' sign and any commas, then convert to float
                parsed_value = float(value.strip("%").replace(",", ""))
                # If value seems to be a percentage over 100, assume it's meant to be in its raw form
                if parsed_value > 100:
                    parsed_value /= 100
                return parsed_value
            except ValueError as e:
                raise ValueError(f"Invalid percentage value: {value}") from e
        return value  # Return the value as is if it's already a valid float or doesn't end with '%'


class PcaViewVersion102(MyBaseModel):
    """PcaView"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    AD_TYPE_NAME: Optional[str] = Field(None, alias="Dimension.AD_TYPE_NAME")
    VIDEO_AD_TYPE_NAME: Optional[str] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_NAME"
    )
    AD_TYPE_ID: Optional[int] = Field(None, alias="Dimension.AD_TYPE_ID")
    VIDEO_AD_TYPE_ID: Optional[int] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_ID"
    )
    SALESPERSON_ID: Optional[int] = Field(
        None, alias="Dimension.SALESPERSON_ID"
    )
    SALESPERSON_NAME: Optional[str] = Field(
        None, alias="Dimension.SALESPERSON_NAME"
    )
    LINE_ITEM_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    LINE_ITEM_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    ORDER_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE: Optional[float] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )
    VIDEO_VIEWERSHIP_START: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_START"
    )
    VIDEO_VIEWERSHIP_FIRST_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_FIRST_QUARTILE"
    )
    VIDEO_VIEWERSHIP_MIDPOINT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_MIDPOINT"
    )
    VIDEO_VIEWERSHIP_THIRD_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_THIRD_QUARTILE"
    )
    VIDEO_VIEWERSHIP_COMPLETE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME"
    )
    VIDEO_VIEWERSHIP_COMPLETION_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETION_RATE"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE"
    )
    VIDEO_TRUEVIEW_SKIP_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_SKIP_RATE"
    )
    VIDEO_TRUEVIEW_VIEWS: Optional[int] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VIEWS"
    )
    VIDEO_TRUEVIEW_VTR: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VTR"
    )
    VIDEO_VIEWERSHIP_VIDEO_LENGTH: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIDEO_LENGTH"
    )
    VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN"
    )
    VIDEO_VIEWERSHIP_ENGAGED_VIEW: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_ENGAGED_VIEW"
    )
    VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE"
    )
    VIDEO_VIEWERSHIP_AUTO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AUTO_PLAYS"
    )
    VIDEO_VIEWERSHIP_CLICK_TO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_CLICK_TO_PLAYS"
    )

    @validator(
        "LINE_ITEM_END_DATE_TIME",
        "LINE_ITEM_START_DATE_TIME",
        "ORDER_START_DATE_TIME",
        "ORDER_END_DATE_TIME",
        pre=True,
        always=True,
    )
    def parse_datetime(cls, value):  # pylint: disable=no-self-argument
        """Parse datetime values"""
        if value == "-" or value == "Unlimited" or not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Invalid datetime value: {value}") from e

    @validator("*", pre=True, always=True)
    def handle_dash_as_null(cls, value):  # pylint: disable=no-self-argument
        """Replace '-' with None"""
        if value == "-":
            return None
        return value

    @validator("VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE", pre=True, always=True)
    def parse_percentage(cls, value):  # pylint: disable=no-self-argument
        """Parse percentage values"""
        if value is None:
            return value
        if isinstance(value, str) and value.endswith("%"):
            try:
                # Remove the '%' sign and any commas, then convert to float
                parsed_value = float(value.strip("%").replace(",", ""))
                # If value seems to be a percentage over 100, assume it's meant to be in its raw form
                if parsed_value > 100:
                    parsed_value /= 100
                return parsed_value
            except ValueError as e:
                raise ValueError(f"Invalid percentage value: {value}") from e
        return value  # Return the value as is if it's already a valid float or doesn't end with '%'


class PcaViewVersion101(MyBaseModel):
    """PcaView"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    SALESPERSON_ID: Optional[int] = Field(
        None, alias="Dimension.SALESPERSON_ID"
    )
    SALESPERSON_NAME: Optional[str] = Field(
        None, alias="Dimension.SALESPERSON_NAME"
    )
    LINE_ITEM_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    LINE_ITEM_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    ORDER_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    ORDER_BOOKED_CPC: Optional[int] = Field(
        None, alias="DimensionAttribute.ORDER_BOOKED_CPC"
    )
    ORDER_BOOKED_CPM: Optional[int] = Field(
        None, alias="DimensionAttribute.ORDER_BOOKED_CPM"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE: Optional[float] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )

    @validator(
        "LINE_ITEM_END_DATE_TIME",
        "LINE_ITEM_START_DATE_TIME",
        "ORDER_START_DATE_TIME",
        "ORDER_END_DATE_TIME",
        pre=True,
        always=True,
    )
    def parse_datetime(cls, value):  # pylint: disable=no-self-argument
        """Parse datetime values"""
        if value == "-" or value == "Unlimited" or not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Invalid datetime value: {value}") from e

    @validator("ORDER_BOOKED_CPC", "ORDER_BOOKED_CPM", pre=True, always=True)
    def handle_dash_as_null(cls, value):  # pylint: disable=no-self-argument
        """Replace '-' with None"""
        if value == "-":
            return None
        return value


class PcaView(MyBaseModel):
    """PcaView"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    LINE_ITEM_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    LINE_ITEM_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    ORDER_END_DATE_TIME: Optional[datetime] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS"
    )
    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE: Optional[float] = Field(
        None, alias="Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )

    @validator(
        "LINE_ITEM_END_DATE_TIME",
        "LINE_ITEM_START_DATE_TIME",
        "ORDER_START_DATE_TIME",
        "ORDER_END_DATE_TIME",
        pre=True,
        always=True,
    )
    def parse_datetime(cls, value):  # pylint: disable=no-self-argument
        """Parse datetime values"""
        if value == "-" or value == "Unlimited" or not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Invalid datetime value: {value}") from e


class PcaHourly(MyBaseModel):
    """PcaHourly"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    HOUR: Optional[int] = Field(None, alias="Dimension.HOUR")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    LINE_ITEM_START_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    LINE_ITEM_END_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    ORDER_END_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    LINE_ITEM_GOAL_QUANTITY: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_GOAL_QUANTITY"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )


class VideoViewership(MyBaseModel):
    """VideoViewership"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    AD_TYPE_NAME: Optional[str] = Field(None, alias="Dimension.AD_TYPE_NAME")
    VIDEO_AD_TYPE_NAME: Optional[str] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_NAME"
    )
    AD_TYPE_ID: Optional[int] = Field(None, alias="Dimension.AD_TYPE_ID")
    VIDEO_AD_TYPE_ID: Optional[int] = Field(
        None, alias="Dimension.VIDEO_AD_TYPE_ID"
    )
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")

    VIDEO_VIEWERSHIP_START: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_START"
    )
    VIDEO_VIEWERSHIP_FIRST_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_FIRST_QUARTILE"
    )
    VIDEO_VIEWERSHIP_MIDPOINT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_MIDPOINT"
    )
    VIDEO_VIEWERSHIP_THIRD_QUARTILE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_THIRD_QUARTILE"
    )
    VIDEO_VIEWERSHIP_COMPLETE: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE"
    )
    VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME"
    )
    VIDEO_VIEWERSHIP_COMPLETION_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_COMPLETION_RATE"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_COUNT"
    )
    VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_TOTAL_ERROR_RATE"
    )
    VIDEO_TRUEVIEW_SKIP_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_SKIP_RATE"
    )
    VIDEO_TRUEVIEW_VIEWS: Optional[int] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VIEWS"
    )
    VIDEO_TRUEVIEW_VTR: Optional[float] = Field(
        None, alias="Column.VIDEO_TRUEVIEW_VTR"
    )
    VIDEO_VIEWERSHIP_VIDEO_LENGTH: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIDEO_LENGTH"
    )
    VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_SKIP_BUTTON_SHOWN"
    )
    VIDEO_VIEWERSHIP_ENGAGED_VIEW: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_ENGAGED_VIEW"
    )
    VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE: Optional[float] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE"
    )
    VIDEO_VIEWERSHIP_AUTO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_AUTO_PLAYS"
    )
    VIDEO_VIEWERSHIP_CLICK_TO_PLAYS: Optional[int] = Field(
        None, alias="Column.VIDEO_VIEWERSHIP_CLICK_TO_PLAYS"
    )

    @validator("*", pre=True, always=True)
    def handle_dash_as_null(cls, value):  # pylint: disable=no-self-argument
        """Replace '-' with None"""
        if value == "-":
            return None
        return value

    @validator("VIDEO_VIEWERSHIP_VIEW_THROUGH_RATE", pre=True, always=True)
    def parse_percentage(cls, value):  # pylint: disable=no-self-argument
        """Parse percentage values"""
        if value is None:
            return value
        if isinstance(value, str) and value.endswith("%"):
            try:
                # Remove the '%' sign and any commas, then convert to float
                parsed_value = float(value.strip("%").replace(",", ""))
                # If value seems to be a percentage over 100, assume it's meant to be in its raw form
                if parsed_value > 100:
                    parsed_value /= 100
                return parsed_value
            except ValueError as e:
                raise ValueError(f"Invalid percentage value: {value}") from e
        return value  # Return the value as is if it's already a valid float or doesn't end with '%'


class PcaGen(MyBaseModel):
    """PcaGen"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    REGION_NAME: Optional[str] = Field(None, alias="Dimension.REGION_NAME")
    COUNTRY_NAME: Optional[str] = Field(None, alias="Dimension.COUNTRY_NAME")
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    REGION_CRITERIA_ID: Optional[int] = Field(
        None, alias="Dimension.REGION_CRITERIA_ID"
    )
    COUNTRY_CRITERIA_ID: Optional[int] = Field(
        None, alias="Dimension.COUNTRY_CRITERIA_ID"
    )
    ORDER_END_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.ORDER_END_DATE_TIME"
    )
    ORDER_START_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.ORDER_START_DATE_TIME"
    )
    LINE_ITEM_GOAL_QUANTITY: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_GOAL_QUANTITY"
    )
    LINE_ITEM_START_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_START_DATE_TIME"
    )
    LINE_ITEM_END_DATE_TIME: Optional[str] = Field(
        None, alias="DimensionAttribute.LINE_ITEM_END_DATE_TIME"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )


class SponCon(MyBaseModel):
    """PcaHourly"""

    model_config = ConfigDict(extra="forbid")

    DATE: date = Field(None, alias="Dimension.DATE")
    ORDER_NAME: Optional[str] = Field(None, alias="Dimension.ORDER_NAME")
    LINE_ITEM_NAME: Optional[str] = Field(
        None, alias="Dimension.LINE_ITEM_NAME"
    )
    DEVICE_CATEGORY_NAME: Optional[str] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_NAME"
    )
    ADVERTISER_NAME: Optional[str] = Field(
        None, alias="Dimension.ADVERTISER_NAME"
    )
    ORDER_ID: Optional[int] = Field(None, alias="Dimension.ORDER_ID")
    LINE_ITEM_ID: Optional[int] = Field(None, alias="Dimension.LINE_ITEM_ID")
    DEVICE_CATEGORY_ID: Optional[int] = Field(
        None, alias="Dimension.DEVICE_CATEGORY_ID"
    )
    ADVERTISER_ID: Optional[int] = Field(None, alias="Dimension.ADVERTISER_ID")
    TOTAL_CODE_SERVED_COUNT: Optional[int] = Field(
        None, alias="Column.TOTAL_CODE_SERVED_COUNT"
    )
    TOTAL_LINE_ITEM_LEVEL_CLICKS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CLICKS"
    )
    TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS"
    )
    TOTAL_LINE_ITEM_LEVEL_WITH_CPD_AVERAGE_ECPM: Optional[int] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_WITH_CPD_AVERAGE_ECPM"
    )
    TOTAL_LINE_ITEM_LEVEL_CTR: Optional[float] = Field(
        None, alias="Column.TOTAL_LINE_ITEM_LEVEL_CTR"
    )
