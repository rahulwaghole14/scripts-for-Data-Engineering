"""
test_unit_validation.py
"""

import pytest
from .validation import (
    VxSubscriptionLog,
)


def test_billing_postal_code_coercion():
    """
    Test that the Billing_postal_code field is coerced to a string
    """
    log = VxSubscriptionLog(
        Days_subscribed=30,
        Regular_price=19.99,
        Auto_renew=True,
        Logins_last_30_days=10,
        Sessions_last_30_days=5,
        Pageviews_last_30_days=100,
        Total_charged=59.99,
        Charge_count=2,
        Total_refunded=0.0,
        Renewed=False,
        Currently_in_grace_period=False,
        record_load_dts="2024-09-27",
        Billing_postal_code=12345,
    )
    assert log.Billing_postal_code == "12345"


def test_postal_code_coercion():
    """
    Test that the Postal_code field is coerced to a string
    """
    log = VxSubscriptionLog(
        Days_subscribed=30,
        Regular_price=19.99,
        Auto_renew=True,
        Logins_last_30_days=10,
        Sessions_last_30_days=5,
        Pageviews_last_30_days=100,
        Total_charged=59.99,
        Charge_count=2,
        Total_refunded=0.0,
        Renewed=False,
        Currently_in_grace_period=False,
        record_load_dts="2024-09-27",
        Postal_code=67890,
    )
    assert log.Postal_code == "67890"


def test_user_id__uid_coercion():
    """
    Test that the User_ID__UID_ field is coerced to a string
    """
    log = VxSubscriptionLog(
        Days_subscribed=30,
        Regular_price=19.99,
        Auto_renew=True,
        Logins_last_30_days=10,
        Sessions_last_30_days=5,
        Pageviews_last_30_days=100,
        Total_charged=59.99,
        Charge_count=2,
        Total_refunded=0.0,
        Renewed=False,
        Currently_in_grace_period=False,
        record_load_dts="2024-09-27",
        User_ID__UID_=98765,
    )
    assert log.User_ID__UID_ == "98765"


def test_record_load_dts_coercion():
    """
    Test that the record_load_dts field is coerced to a string
    """
    log = VxSubscriptionLog(
        Days_subscribed=30,
        Regular_price=19.99,
        Auto_renew=True,
        Logins_last_30_days=10,
        Sessions_last_30_days=5,
        Pageviews_last_30_days=100,
        Total_charged=59.99,
        Charge_count=2,
        Total_refunded=0.0,
        Renewed=False,
        Currently_in_grace_period=False,
        record_load_dts=20240927,
    )
    assert log.record_load_dts == "20240927"


def test_optional_fields_none():
    """
    Test that optional fields are set to None when not provided
    """
    log = VxSubscriptionLog(
        Days_subscribed=30,
        Regular_price=19.99,
        Auto_renew=True,
        Logins_last_30_days=10,
        Sessions_last_30_days=5,
        Pageviews_last_30_days=100,
        Total_charged=59.99,
        Charge_count=2,
        Total_refunded=0.0,
        Renewed=False,
        Currently_in_grace_period=False,
        record_load_dts="2024-09-27",
        Billing_postal_code=None,
        Postal_code=None,
        User_ID__UID_=None,
    )
    assert log.Billing_postal_code is None
    assert log.Postal_code is None
    assert log.User_ID__UID_ is None
    assert log.record_load_dts == "2024-09-27"


if __name__ == "__main__":
    pytest.main()
