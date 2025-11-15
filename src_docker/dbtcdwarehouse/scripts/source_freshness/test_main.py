"""
unit tests for main.py
"""

from .main import clean_ansi_codes, parse_failure_messages


def test_clean_ansi_codes():
    """
    test clean_ansi_codes
    """

    assert clean_ansi_codes("test") == "test"
    assert clean_ansi_codes("\x1b[0mtest") == "test"


def test_parse_failure_messages():
    """
    test parse_failure_messages
    """

    assert parse_failure_messages(["test"]) == []
    assert parse_failure_messages(["test", "error: test"]) == ["error: test"]
    assert parse_failure_messages(["error: test"]) == ["error: test"]
    assert parse_failure_messages(["error: test", "error: test"]) == [
        "error: test",
        "error: test",
    ]
