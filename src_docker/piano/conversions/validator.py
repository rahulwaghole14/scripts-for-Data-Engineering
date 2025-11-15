"""
validators
"""

from datetime import date
from typing import Optional
from common.validation.validators import MyBaseModel


class ConversionActionModel(MyBaseModel):
    """ConversionActionModel"""

    Canvas_action: Optional[str] = None
    Context: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: float
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionCategoryModel(MyBaseModel):
    """ConversionCategoryModel"""

    Category: Optional[str] = None
    Context: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: float
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionSourceModel(MyBaseModel):
    """ConversionSourceModel"""

    Source: Optional[str] = None
    Context: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: float
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionNoGroupingModel(MyBaseModel):
    """ConversionNoGroupingModel"""

    Context: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: float
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionTermModel(MyBaseModel):
    """ConversionTermModel"""

    Template: Optional[str] = None
    Offer: Optional[str] = None
    Term: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    date_at: date
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    report_type: Optional[str] = None


class ConversionPageModel(MyBaseModel):
    """ConversionPageModel"""

    URL: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: Optional[float] = None
    date_at: date
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    report_type: Optional[str] = None


class ConversionTemplateModel(MyBaseModel):
    """ConversionTemplateModel"""

    Template: Optional[str] = None
    Template_variant: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: Optional[float] = None
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionDeviceModel(MyBaseModel):
    """ConversionDeviceModel"""

    Device: Optional[str] = None
    Operating_System: Optional[str] = None
    Browser: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: Optional[float] = None
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionLocationModel(MyBaseModel):
    """ConversionLocationModel"""

    Country: Optional[str] = None
    Location: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: Optional[float] = None
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


class ConversionTermTypeModel(MyBaseModel):
    """ConversionTermTypeModel"""

    Term_type: Optional[str] = None
    Term_name: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    date_at: date
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    report_type: Optional[str] = None


class ConversionCampaignCodeModel(MyBaseModel):
    """ConversionCampaignCodeModel"""

    Campaign_code: Optional[str] = None
    Attributed_conversions: int | None
    Average_time_to_conversion_seconds: int | None
    date_at: date
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    report_type: Optional[str] = None


class ConversionPromotionModel(MyBaseModel):
    """ConversionPromotionModel"""

    Promotion: Optional[str] = None
    Conversions: int | None
    date_at: date
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    report_type: Optional[str] = None


class ConversionTagModel(MyBaseModel):
    """ConversionTagModel"""

    Tag: Optional[str] = None
    Exposures: int
    Conversions: int
    Conversion_rate: Optional[float] = None
    Currency: Optional[str] = None
    Value: Optional[float] = None
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    date_at: date


# Map dataframe names to Pydantic models
model_map = {
    "conversion_action": ConversionActionModel,
    "conversion_category": ConversionCategoryModel,
    "conversion_source": ConversionSourceModel,
    "conversion_template": ConversionTemplateModel,
    "conversion_device": ConversionDeviceModel,
    "conversion_location": ConversionLocationModel,
    "conversion_tags": ConversionTagModel,
    "conversion_page": ConversionPageModel,
    "conversion_no_grouping": ConversionNoGroupingModel,
    "conversion_term": ConversionTermModel,
    "conversion_termType": ConversionTermTypeModel,
    "conversion_campaignCode": ConversionCampaignCodeModel,
    "conversion_promotion": ConversionPromotionModel,
    "conversion_tag": ConversionTagModel,
}


def get_model_from_filename(filename: str):
    """gets the correct model to compare to for each piano file"""
    parts = filename.split("-")
    base_name = "-".join(parts[:2]) if len(parts) > 2 else parts[0]
    base_name = base_name.replace("-", "_")

    if base_name not in model_map:
        raise ValueError(
            f"No validation model defined for dataframe '{filename}'."
        )

    return model_map[base_name], base_name
