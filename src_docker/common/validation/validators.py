"""
pydantic validation functions
"""

import sys
import logging
from datetime import datetime, date
from math import isnan
from typing import Any
from pydantic import ValidationError, BaseModel, validator
import pandas as pd


class MyBaseModel(BaseModel):
    """Use a root validator to apply logic across all fields"""

    @validator(
        "*", pre=True, allow_reuse=True
    )  # Applies to all fields before their own validators
    @classmethod
    def convert_nan_to_none(cls, value: Any) -> Any:
        """Check if value is float and NaN, then convert to None"""
        if isinstance(value, float) and isnan(value):
            return None
        return value


def validate_dataframe(df: pd.DataFrame, model):
    """
    function: validates a pandas dataframe against pydantic model/basemodel class
    return: returns validated data into list of dicts
    """
    validated_data = []
    for _, row in df.iterrows():
        try:
            validated_model = model(**row.to_dict())
            validated_data.append(validated_model.dict())
        except ValidationError as e:
            logging.error("Validation error: %s", e.json())
            sys.exit("Validation error. See logs for details.")

    return validated_data


def validate_list_of_dicts(data_list: list, model):
    """
    Validates a list of dictionaries against a Pydantic model.

    :param data_list: List of dictionaries to validate.
    :param model: Pydantic model class used for validation.
    :return: A list of validated data as dictionaries.
    """
    validated_data = []
    for data_dict in data_list:
        try:
            validated_model = model(**data_dict)
            validated_data.append(validated_model.dict())
        except ValidationError as e:
            logging.error("Validation error: %s", e.json())
            sys.exit("Validation error. See logs for details.")

    return validated_data


def serialize_datetime_to_isoformat(value):
    """
    Recursively converts datetime objects to their ISO format string representation.

    :param value: The value to check and potentially serialize.
    :return: The value, with datetime objects converted to ISO strings if necessary.
    """
    if isinstance(value, dict):
        return {
            k: serialize_datetime_to_isoformat(v) for k, v in value.items()
        }
    if isinstance(value, list):
        return [serialize_datetime_to_isoformat(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def validate_list_of_dicts_serialised(
    data_list: list, model, identifier_field: str = None
):
    """
    Validates a list of dictionaries against a
    Pydantic model and serializes datetime objects to ISO format.

    :param data_list: List of dictionaries to validate.
    :param model: Pydantic model class used for validation.
    :param identifier_field: Field name to include in error logs for identification.
    :return: A list of validated and serialized data as dictionaries.
    """
    validated_data = []
    for data_dict in data_list:
        try:
            validated_model = model(**data_dict)
            # Convert the model to a dictionary and serialize datetime objects
            model_dict = validated_model.dict()
            serialized_data = serialize_datetime_to_isoformat(model_dict)
            validated_data.append(serialized_data)
        except ValidationError as e:
            # Extract the identifier field from the data_dict
            identifier_value = (
                data_dict.get(identifier_field, "Unknown")
                if identifier_field
                else "Unknown"
            )
            logging.error(
                "Validation error for %s=%s: %s",
                identifier_field if identifier_field else "identifier",
                identifier_value,
                e.json(),
            )
            sys.exit("Validation error. See logs for details.")

    return validated_data
