from typing import Type, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from typing import Optional
import pydantic
from pydantic import create_model, BaseModel, ValidationError
from common.logging.logger import logger
import pyodbc


def sql_type_to_python_type(sql_type: str, is_nullable: bool):
    mapping = {
        "int": (int, ...),
        "bigint": (int, ...),
        "smallint": (int, ...),
        "tinyint": (int, ...),
        "varchar": (str, ...),
        "char": (str, ...),
        "datetime": (datetime, ...),
        "smalldatetime": (datetime, ...),
        "date": (date, ...),
        "bit": (bool, ...),
        "decimal": (Decimal, ...),  # Confirming mapping for decimal
        "float": (float, ...),  # Added mapping for float
        "numeric": (Decimal, ...),
    }
    python_type, extra = mapping.get(
        sql_type.lower(), (str, ...)
    )  # Default to str if unknown
    if is_nullable:
        return (Optional[python_type], extra)
    return (python_type, extra)


def fetch_and_create_dynamic_model(cursor, schema_name: str, table_name: str):
    cursor.execute(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}';
    """
    )

    fields = {
        row.COLUMN_NAME: sql_type_to_python_type(
            row.DATA_TYPE, row.IS_NULLABLE == "YES"
        )
        for row in cursor.fetchall()
    }

    # Use both schema and table name to create a unique model name, replacing potential issues in names
    safe_schema_name = schema_name.replace(".", "_").replace("-", "_")
    safe_table_name = table_name.replace(".", "_").replace("-", "_")
    model_name = f"{safe_schema_name}_{safe_table_name}_Model"

    # Creating a dynamic Pydantic model
    DynamicModel = create_model(
        model_name,
        **{
            field_name: (field_type, None)
            for field_name, (field_type, _) in fields.items()
        },
        __base__=BaseModel,
    )

    return DynamicModel


def validate_data_with_pydantic(model, row_data):
    try:
        validated_data = model.parse_obj(row_data)
        return validated_data.dict(exclude_unset=True)
    except ValidationError as e:
        logger.error(f"Validation error for row: {row_data}\n{e}")
        return None
