"""
Test cases for bigquery module
"""

import unittest
from typing import List
from pydantic import BaseModel

from .bigquery import is_pydantic_model, MyBaseModel


class TestIsPydanticModel(unittest.TestCase):
    """
    Test cases for is_pydantic_model function
    """

    def test_with_my_base_model_subclass(self):
        """
        Test with MyBaseModel subclass
        """

        class MyModel(MyBaseModel):
            """
            MyModel class
            """

            field1: int

        self.assertTrue(is_pydantic_model(MyModel))

    def test_with_base_model_subclass(self):
        """
        Test with BaseModel subclass
        """

        class MyModel(BaseModel):
            """
            MyModel class
            """

            field1: int

        self.assertFalse(is_pydantic_model(MyModel))

    def test_with_builtin_type(self):
        """
        Test with built-in type
        """
        self.assertFalse(is_pydantic_model(int))

    def test_with_non_class(self):
        """
        Test with non-class
        """
        self.assertFalse(is_pydantic_model(123))

    def test_with_generic_type(self):
        """
        Test with generic type
        """
        self.assertFalse(is_pydantic_model(List[int]))


if __name__ == "__main__":
    unittest.main()
