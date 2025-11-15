import json
import logging
import math
import os
import pandas as pd
import sys
import pydantic
import pytz
import base64
from pydantic import BaseModel, Field, field_validator, create_model, validator, root_validator, ValidationError, field_validator, ValidationInfo
from datetime import datetime, date, time, timezone
from typing import Optional, Any, List, Type, Union, Dict, get_args, get_origin, Tuple
from google.cloud import bigquery
from pydantic_core import core_schema
import math


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.logging.logger import logger

logger = logger(
    "neighbourly_data_validation",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


class DynamicModel(BaseModel):
    @field_validator('*', mode='before')
    def convert_fields(cls, value: Any):
        # Handle datetime conversion
        if isinstance(value, str) and ' UTC' in value:
            try:
                return datetime.fromisoformat(value.replace(' UTC', '+00:00'))
            except ValueError:
                return None  # Set to None if the datetime is not parseable

        # Handle NaN for integer fields
        if isinstance(value, float) and math.isnan(value):
            return None  # Convert NaN to None
        # Handle NaT
        if value == 'NaT':
            return None


        return value
    
def create_dynamic_table_model(table_name: str, schema_info: List[Tuple[str, str]]) -> Type[BaseModel]:
    field_definitions = {}
    for column_name, column_type in schema_info:
        column_type = column_type.lower()
        
        if column_type in ['int', 'smallint', 'mediumint', 'bigint']:
            field_definitions[column_name] = (Optional[int], None)
        elif column_type.startswith('tinyint(1)'):
            field_definitions[column_name] = (Optional[bool], None)
        elif column_type.startswith('tinyint'):
            field_definitions[column_name] = (Optional[int], None)
        elif column_type in ['float', 'double', 'decimal']:
            field_definitions[column_name] = (Optional[float], None)
        elif column_type in ['varchar', 'char', 'text', 'longtext']:
            field_definitions[column_name] = (Optional[str], None)
        elif column_type == 'date':
            field_definitions[column_name] = (Optional[date], None)
        elif column_type in ['datetime', 'timestamp', 'date', 'time']:
            field_definitions[column_name] = (Optional[datetime], None)
        elif column_type in ['blob', 'binary', 'varbinary']:
            field_definitions[column_name] = (Optional[bytes], None)
        else:
            field_definitions[column_name] = (Optional[str], None)  # Default to string for unknown types

    model = create_model(
        f"{table_name.capitalize()}Model", 
        __base__=DynamicModel,  # Use the DynamicModel as the base to include the custom validator
        **field_definitions
    )

    return model

def parse_and_format_datetime(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    elif isinstance(value, str):
        try:
            # Try parsing as datetime
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.isoformat()
        except ValueError:
            try:
                # Try parsing as date
                d = datetime.strptime(value, "%Y-%m-%d").date()
                return d.isoformat()
            except ValueError:
                try:
                    # Try parsing datetime with UTC
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S UTC")
                    return dt.replace(tzinfo=pytz.UTC).isoformat()
                except ValueError:
                    # If all parsing attempts fail, return the original value
                    return value
    return value


def validate_data(model: Type[BaseModel], data: List[dict]) -> List[dict]:
    validated_data = []
    for row in data:
        try:
            # Pre-process the row to handle binary data
            processed_row = {}
            for key, value in row.items():
                if isinstance(value, bytes):
                    # Encode binary data to base64
                    processed_row[key] = base64.b64encode(value).decode('utf-8')
                else:
                    processed_row[key] = value

            validated_row = model(**processed_row).model_dump(exclude_unset=True)
            # Convert all potential datetime fields to ISO format for JSON serialization
            for key, value in validated_row.items():
                if value == 'NaT' or value == '':
                    validated_row[key] = None
                elif not isinstance(value, str):  # Skip already processed binary data
                    validated_row[key] = parse_and_format_datetime(value)
            validated_data.append(validated_row)
        except ValidationError as e:
            logger.warning(f"Validation error in row: {row}")
            logger.warning(f"Error details: {e}")
            cleaned_row = {k: None if k in str(e) or v in ['NaT', ''] else v for k, v in processed_row.items()}
            try:
                validated_row = model(**cleaned_row).model_dump(exclude_unset=True)
                for key, value in validated_row.items():
                    if not isinstance(value, str):  # Skip already processed binary data
                        validated_row[key] = parse_and_format_datetime(value)
                validated_data.append(validated_row)
            except ValidationError as e2:
                logger.error(f"Failed to salvage row even after cleaning: {cleaned_row}")
                logger.error(f"Error details: {e2}")
    return validated_data