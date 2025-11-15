"""
SQLModel validation functions
"""

import logging
from typing import List, Type
from sqlmodel import SQLModel
from sqlmodel._compat import SQLModelConfig
from pydantic import ValidationError

# pylint: disable=too-few-public-methods
class SQLModelValidation(SQLModel):
    """
    Helper class to allow for validation in SQLModel classes with table=True
    """

    model_config = SQLModelConfig(
        from_attributes=True,
        validate_assignment=True,
        extra="forbid",  # Disallow extra fields
    )


def validate_sqlmodel_list_of_dicts_serialised(
    data_list: List[dict], model_class: Type[SQLModel]
) -> List[SQLModel]:
    """
    Validate a list of dictionaries against a SQLModel class.
    """
    validated_data = []
    for item in data_list:
        try:
            validated_item = model_class(**item)
            validated_data.append(validated_item)
        except ValidationError as e:
            logging.error("Validation error for item %s: %s", item, e)
    return validated_data
