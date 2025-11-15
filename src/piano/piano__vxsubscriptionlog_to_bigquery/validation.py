"""
validators
"""

from typing import Optional
from pydantic import ConfigDict, validator
from a_common.validation.validators import MyBaseModel


class VxSubscriptionLog(MyBaseModel):
    """VxSubscriptionLogModel"""

    model_config = ConfigDict(extra="forbid")

    Subscription_ID: Optional[str] = None
    Summary: Optional[str] = None
    Create_date: Optional[str] = None
    Start_date: Optional[str] = None
    End_date: Optional[str] = None
    Days_subscribed: int
    Subscription_status: Optional[str] = None
    Upgrade_status: Optional[str] = None
    Trial_status: Optional[str] = None
    Trial_period_end_date: Optional[str] = None
    Trial_price: Optional[float] = None
    Regular_price: float
    Auto_renew: bool
    Auto_renew_disablement_date: Optional[str] = None
    Last_active_date: Optional[str] = None
    Logins_last_30_days: int
    Sessions_last_30_days: int
    Pageviews_last_30_days: int
    Billing_period: Optional[str] = None
    Total_charged: float
    Charge_count: int
    Total_refunded: float
    Payment_source: Optional[str] = None
    Last_billing_date: Optional[str] = None
    Next_billing_date: Optional[str] = None
    Renewed: bool
    Currently_in_grace_period: bool
    Grace_period_start_date: Optional[str] = None
    Grace_period_extended_to: Optional[str] = None
    User_ID__UID_: Optional[str] = None
    User_email: Optional[str] = None
    First_name: Optional[str] = None
    Last_name: Optional[str] = None
    Access_expiration_date: Optional[str] = None
    Resource_ID__RID_: Optional[str] = None
    Resource_name: Optional[str] = None
    Term_ID: Optional[str] = None
    Term_name: Optional[str] = None
    Term_type: Optional[str] = None
    Template_ID: Optional[str] = None
    Template_name: Optional[str] = None
    Offer_ID: Optional[str] = None
    Offer_name: Optional[str] = None
    Experience_ID: Optional[str] = None
    Experience_name: Optional[str] = None
    Campaign_codes: Optional[str] = None
    Promo_code: Optional[str] = None
    Conversion_city: Optional[str] = None
    Conversion_state: Optional[str] = None
    Conversion_country: Optional[str] = None
    Device: Optional[str] = None
    URL: Optional[str] = None
    Cleaned_URL: Optional[str] = None
    UTM_parameters: Optional[str] = None
    Name__first_and_last_: Optional[str] = None
    Company_name: Optional[str] = None
    Address_1: Optional[str] = None
    Address_2: Optional[str] = None
    Address_city: Optional[str] = None
    Address_state: Optional[str] = None
    Address_country: Optional[str] = None
    Postal_code: Optional[str] = None
    Phone: Optional[float] = None
    PSC_subscriber_number: Optional[str] = None
    Tax: Optional[float] = None
    Tax_base: Optional[float] = None
    Tax_rate: Optional[float] = None
    Tax_country: Optional[str] = None
    Tax_state_province: Optional[str] = None
    Billing_postal_code: Optional[str] = None
    Shared_subscriptions: Optional[int] = None
    Migrated_to_Piano: Optional[bool] = None
    Migrated_date: Optional[str] = None
    Created_via_upgrade: Optional[bool] = None
    Upgrade_from___subscription_ID: Optional[str] = None
    Period_name: Optional[str] = None
    Access_period: Optional[str] = None
    Period_count: Optional[float] = None
    report_type: Optional[str] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    record_load_dts: str

    @validator("Billing_postal_code", pre=True)
    def coerce_to_str_billing_postal_code(
        cls, v
    ):  # pylint: disable=no-self-argument
        """coerce to string"""
        return str(v) if v else None

    @validator("Postal_code", pre=True)
    def coerce_to_str_postal_code(cls, v):  # pylint: disable=no-self-argument
        """coerce to string"""
        return str(v) if v else None

    @validator("User_ID__UID_", pre=True)
    def coerce_to_str_user_id__uid(cls, v):  # pylint: disable=no-self-argument
        """coerce to string"""
        return str(v) if v else None

    # parse record_load_dts into str
    @validator("record_load_dts", pre=True)
    def coerce_to_str_record_load_dts(
        cls, v
    ):  # pylint: disable=no-self-argument
        """coerce to string"""
        return str(v) if v else None
