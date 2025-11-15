"""
src/piano/piano__vxsubscriptionlog_to_bigquery/tests/test_retry.py
"""

import unittest
from unittest.mock import Mock, patch
import logging

# Import the retry decorator and the custom exception from the correct module
from .main import retry, UnexpectedJobStatusError


class TestRetryDecorator(unittest.TestCase):
    """Test cases for the retry decorator."""

    def setUp(self):
        # Configure logging to capture log outputs for assertions if needed
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("test_retry")

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_success_on_first_attempt(self, mock_sleep):
        """Test that the function succeeds on the first attempt without retries."""
        mock_func = Mock()
        mock_func.return_value = {"job_status": "FINISHED"}

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        result = decorated_func(
            "status_endpoint",
            "request_endpoint",
            "app_id",
            "app_token",
            "2023-01-01",
            "2023-01-02",
        )

        # Assert the function was called once
        mock_func.assert_called_once_with(
            "status_endpoint",
            "request_endpoint",
            "app_id",
            "app_token",
            "2023-01-01",
            "2023-01-02",
        )

        # Assert no sleep was called
        mock_sleep.assert_not_called()

        # Assert the result is as expected
        self.assertEqual(result, {"job_status": "FINISHED"})

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_success_after_retries(self, mock_sleep):
        """Test that the function succeeds after a few retries."""
        mock_func = Mock()
        # First two calls return a failure status, third call returns success
        mock_func.side_effect = [
            {"job_status": "FAILED"},
            {"job_status": "INTERNAL_ERROR"},
            {"job_status": "FINISHED"},
        ]

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        result = decorated_func(
            "status_endpoint",
            "request_endpoint",
            "app_id",
            "app_token",
            "2023-01-01",
            "2023-01-02",
        )

        # Assert the function was called three times
        self.assertEqual(mock_func.call_count, 3)

        # Assert sleep was called twice (between retries)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)  # First delay
        mock_sleep.assert_any_call(2)  # Second delay (1 * backoff=2)

        # Assert the result is as expected
        self.assertEqual(result, {"job_status": "FINISHED"})

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_failure_after_max_retries(self, mock_sleep):
        """Test that the function raises an exception after maximum retries."""
        mock_func = Mock()
        # All calls return a failure status
        mock_func.side_effect = [
            {"job_status": "FAILED"},
            {"job_status": "INTERNAL_ERROR"},
            {"job_status": "FAILED"},
        ]

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        with self.assertRaises(RuntimeError) as context:
            decorated_func(
                "status_endpoint",
                "request_endpoint",
                "app_id",
                "app_token",
                "2023-01-01",
                "2023-01-02",
            )

        # Assert the function was called three times
        self.assertEqual(mock_func.call_count, 3)

        # Assert sleep was called twice
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

        # Assert the exception message is correct
        self.assertIn(
            "Report generation failed with status: FAILED",
            str(context.exception),
        )

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_unexpected_status(self, mock_sleep):
        """Test that the function raises an exception on unexpected job status."""
        mock_func = Mock()
        # Function returns an unexpected status
        mock_func.return_value = {"job_status": "UNKNOWN_STATUS"}

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        with self.assertRaises(UnexpectedJobStatusError) as context:
            decorated_func(
                "status_endpoint",
                "request_endpoint",
                "app_id",
                "app_token",
                "2023-01-01",
                "2023-01-02",
            )

        # Assert the function was called once (no retry for unexpected status)
        mock_func.assert_called_once_with(
            "status_endpoint",
            "request_endpoint",
            "app_id",
            "app_token",
            "2023-01-01",
            "2023-01-02",
        )

        # Assert no sleep was called
        mock_sleep.assert_not_called()

        # Assert the exception message is correct
        self.assertIn(
            "Unexpected job status: UNKNOWN_STATUS", str(context.exception)
        )

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_exception_handling(self, mock_sleep):
        """Test that the function retries when an exception is raised."""
        mock_func = Mock()
        # First two calls raise exceptions, third call succeeds
        mock_func.side_effect = [
            RuntimeError("Network error"),
            RuntimeError("Timeout error"),
            {"job_status": "FINISHED"},
        ]

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        result = decorated_func(
            "status_endpoint",
            "request_endpoint",
            "app_id",
            "app_token",
            "2023-01-01",
            "2023-01-02",
        )

        # Assert the function was called three times
        self.assertEqual(mock_func.call_count, 3)

        # Assert sleep was called twice
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

        # Assert the result is as expected
        self.assertEqual(result, {"job_status": "FINISHED"})

    @patch(
        "src.piano.piano__vxsubscriptionlog_to_bigquery.main.time.sleep",
        return_value=None,
    )
    def test_exception_after_max_retries(self, mock_sleep):
        """
        Test that the function raises an exception
        after maximum retries when exceptions persist.
        """
        mock_func = Mock()
        # All calls raise exceptions
        mock_func.side_effect = RuntimeError("Persistent error")

        decorated_func = retry(max_attempts=3, delay=1, backoff=2)(mock_func)

        with self.assertRaises(RuntimeError) as context:
            decorated_func(
                "status_endpoint",
                "request_endpoint",
                "app_id",
                "app_token",
                "2023-01-01",
                "2023-01-02",
            )

        # Assert the function was called three times
        self.assertEqual(mock_func.call_count, 3)

        # Assert sleep was called twice
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

        # Assert the exception message is correct
        self.assertIn("Persistent error", str(context.exception))


if __name__ == "__main__":
    unittest.main()
