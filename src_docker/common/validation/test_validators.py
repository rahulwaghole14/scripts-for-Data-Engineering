"""
test_functionfile.py
"""

import unittest
from unittest.mock import patch
import logging
import sys
from datetime import datetime, date
from pydantic import BaseModel

from .validators import validate_list_of_dicts_serialised

# Sample Pydantic model for testing
class UserModel(BaseModel):
    user_id: int
    name: str
    signup_date: datetime
    preferences: dict = (
        None  # Including a nested dict to test recursive serialization
    )


class TestValidateListOfDictsSerialised(unittest.TestCase):
    def setUp(self):
        self.valid_data = [
            {
                "user_id": 1,
                "name": "Alice",
                "signup_date": datetime(2023, 1, 1, 12, 0, 0),
                "preferences": {
                    "newsletter": True,
                    "timezone": "UTC",
                    "last_login": datetime(2023, 1, 10, 9, 30, 0),
                },
            },
            {
                "user_id": 2,
                "name": "Bob",
                "signup_date": datetime(2023, 1, 2, 13, 0, 0),
                "preferences": {
                    "newsletter": False,
                    "timezone": "PST",
                    "last_login": datetime(2023, 1, 11, 10, 45, 0),
                },
            },
        ]

        self.invalid_data = [
            {
                "user_id": "not_an_int",  # Invalid user_id
                "name": "Charlie",
                "signup_date": datetime(2023, 1, 3, 14, 0, 0),
                "preferences": {},
            }
        ]

    def test_valid_data(self):
        """
        Test that valid data is processed correctly.
        """
        result = validate_list_of_dicts_serialised(
            self.valid_data, UserModel, identifier_field="user_id"
        )
        expected = [
            {
                "user_id": 1,
                "name": "Alice",
                "signup_date": "2023-01-01T12:00:00",
                "preferences": {
                    "newsletter": True,
                    "timezone": "UTC",
                    "last_login": "2023-01-10T09:30:00",
                },
            },
            {
                "user_id": 2,
                "name": "Bob",
                "signup_date": "2023-01-02T13:00:00",
                "preferences": {
                    "newsletter": False,
                    "timezone": "PST",
                    "last_login": "2023-01-11T10:45:00",
                },
            },
        ]
        self.assertEqual(result, expected)

    @patch("src_docker.common.validation.validators.logging.error")
    def test_invalid_data_logging(self, mock_logging_error):
        """
        Test that invalid data logs an error with the correct identifier field.
        """
        with self.assertRaises(SystemExit):
            validate_list_of_dicts_serialised(
                self.invalid_data, UserModel, identifier_field="user_id"
            )

        mock_logging_error.assert_called_once()
        args, kwargs = mock_logging_error.call_args
        logged_message = args[0] % args[1:]
        self.assertIn(
            "Validation error for user_id=not_an_int", logged_message
        )

    @patch("src_docker.common.validation.validators.sys.exit")
    @patch("src_docker.common.validation.validators.logging.error")
    def test_invalid_data_sys_exit(self, mock_logging_error, mock_sys_exit):
        """
        Test that the function calls sys.exit on validation error.
        """
        validate_list_of_dicts_serialised(
            self.invalid_data, UserModel, identifier_field="user_id"
        )
        mock_sys_exit.assert_called_once_with(
            "Validation error. See logs for details."
        )

    @patch("src_docker.common.validation.validators.logging.error")
    def test_identifier_field_missing(self, mock_logging_error):
        """
        Test that if identifier_field is missing in data_dict, 'Unknown' is logged.
        """
        data_with_missing_id = [
            {
                "name": "Dave",
                "signup_date": datetime(2023, 1, 4, 15, 0, 0),
                "preferences": {},
            }
        ]

        with self.assertRaises(SystemExit):
            validate_list_of_dicts_serialised(
                data_with_missing_id, UserModel, identifier_field="user_id"
            )

        mock_logging_error.assert_called_once()
        args, kwargs = mock_logging_error.call_args
        # Format the logged message
        logged_message = args[0] % args[1:]
        self.assertIn("Validation error for user_id=Unknown", logged_message)

    @patch("src_docker.common.validation.validators.logging.error")
    def test_no_identifier_field(self, mock_logging_error):
        """
        Test that if identifier_field is not provided, 'identifier' is used in the log.
        """
        with self.assertRaises(SystemExit):
            validate_list_of_dicts_serialised(self.invalid_data, UserModel)

        mock_logging_error.assert_called_once()
        args, kwargs = mock_logging_error.call_args
        logged_message = args[0] % args[1:]
        self.assertIn(
            "Validation error for identifier=Unknown", logged_message
        )

    def test_empty_data_list(self):
        """
        Test that an empty data list returns an empty list.
        """
        result = validate_list_of_dicts_serialised(
            [], UserModel, identifier_field="user_id"
        )
        self.assertEqual(result, [])

    def test_multiple_invalid_records(self):
        """
        Test that the function exits on the first validation error encountered.
        """
        mixed_data = self.valid_data + self.invalid_data
        with self.assertRaises(SystemExit):
            validate_list_of_dicts_serialised(
                mixed_data, UserModel, identifier_field="user_id"
            )

    def test_serialization_of_datetime(self):
        """
        Test that datetime fields are serialized to ISO format, including nested structures.
        """
        data_with_datetime = [
            {
                "user_id": 3,
                "name": "Eve",
                "signup_date": datetime(2023, 1, 5, 16, 0, 0),
                "preferences": {
                    "newsletter": True,
                    "timezone": "EST",
                    "last_login": datetime(2023, 1, 12, 11, 15, 0),
                },
            }
        ]
        result = validate_list_of_dicts_serialised(
            data_with_datetime, UserModel, identifier_field="user_id"
        )
        expected = [
            {
                "user_id": 3,
                "name": "Eve",
                "signup_date": "2023-01-05T16:00:00",
                "preferences": {
                    "newsletter": True,
                    "timezone": "EST",
                    "last_login": "2023-01-12T11:15:00",
                },
            }
        ]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
