"""
validators for piano subscriptions endpoint
"""
# pylint: disable=line-too-long

from typing import Optional
from typing import List
from pydantic import ConfigDict, EmailStr
from a_common.validation.validators import MyBaseModel


class UserSubscriptionAccount(MyBaseModel):
    """UserSubscriptionAccount"""

    account_id: Optional[
        str
    ] = None  # (string) : The ID of the shared subscription account
    user_id: Optional[
        str
    ] = None  # user_id  (string) : The shared subscription user's ID
    email: Optional[
        EmailStr
    ] = None  # email  (string) : The shared subscription user's email
    first_name: Optional[
        str
    ] = None  # first_name  (string) : The shared subscription user's first name
    last_name: Optional[
        str
    ] = None  # last_name  (string) : The shared subscription user's last name
    personal_name: Optional[
        str
    ] = None  # personal_name  (string) : The shared subscription user's personal name
    redeemed: Optional[
        int
    ] = None  # redeemed  (date-time) : The redeem date of the shared subscription


class Resource(MyBaseModel):
    """Resource"""

    rid: Optional[str] = None  #  (string) : The resource ID
    aid: Optional[str] = None  #  (string) : The application ID
    deleted: Optional[
        bool
    ] = None  #  (boolean) : Whether the object is deleted
    disabled: Optional[
        bool
    ] = None  #  (boolean) : Whether the object is disabled
    create_date: Optional[int] = None  #  (date-time) : The creation date
    update_date: Optional[int] = None  #  (date-time) : The update date
    publish_date: Optional[int] = None  #  (date-time) : The publish date
    name: Optional[str] = None  #  (string) : The name
    description: Optional[str] = None  #  (string) : The resource description
    image_url: Optional[
        str
    ] = None  #  (string) : The URL of the resource image
    type: Optional[
        str
    ] = None  #  (string) : The type of the resource (0: Standard, 4: Bundle)
    type_label: Optional[
        str
    ] = None  #  (string) : The resource type label ("Standard" or "Bundle")
    bundle_type: Optional[str] = None  #  (string) : The resource bundle type
    bundle_type_label: Optional[
        str
    ] = None  #  (string) : The bundle type label
    purchase_url: Optional[
        str
    ] = None  #  (string) : The URL of the purchase page
    resource_url: Optional[str] = None  #  (string) : The URL of the resource
    external_id: Optional[
        str
    ] = None  #  (string) : The external ID; defined by the client
    is_fbia_resource: Optional[
        bool
    ] = None  #  (boolean) : Enable the resource for Facebook Subscriptions in Instant Articles


class Period(MyBaseModel):
    """Period"""

    name: Optional[str] = None  #  (string) : The period name
    period_id: Optional[str] = None  #  (string) : The period ID
    sell_date: int  #  (date-time) : The sell date of the period
    begin_date: int  #  (date-time) : The date when the period begins
    end_date: int  #  (date-time) : The date when the period ends
    deleted: bool  #  (boolean) : Whether the object is deleted
    create_date: int  #  (date-time) : The creation date
    update_date: int  #  (date-time) : The update date
    is_sale_started: bool  #  (boolean) : Whether sale is started for the period
    is_active: bool  #  (boolean) : Whether the period is active. A period is in the Active state when the sell date is passed but the end date is not reached


class Schedule(MyBaseModel):
    """Schedule"""

    aid: Optional[str] = None  #  (string) : The application ID
    name: Optional[str] = None  #  (string) : The schedule name
    schedule_id: Optional[str] = None  #  (string) : The schedule ID
    deleted: bool  #  (boolean) : Whether the object is deleted
    create_date: int  #  (date-time) : The creation date
    update_date: int  #  (date-time) : The update date
    periods: List[Period]  #  (Array[Period])


class ExternalAPIField(MyBaseModel):
    """ExternalAPIField"""

    field_name: Optional[
        str
    ] = None  #  (string) : The name of the field to be used to submit to the external system
    field_title: Optional[
        str
    ] = None  #  (string) : The title of the field to be displayed to the user
    description: Optional[
        str
    ] = None  #  (string) : The field description, some information about what information should be entered
    mandatory: bool  #  (boolean) : Whether the field is required
    hidden: bool  #  (boolean) : Whether the field will be submitted hiddenly from the user, default value is required
    default_value: Optional[
        str
    ] = None  #  (string) : Default value for the field. It will be pre-entered on the form
    order: int  #  (int32) : Field order in the list
    type: Optional[str] = None  #  (string) : Field type
    editable: Optional[
        str
    ] = None  #  (string) : Whether the object is editable


class Region(MyBaseModel):
    """Region"""

    region_name: Optional[
        str
    ] = None  #  (string) : The name of the country region
    region_code: Optional[
        str
    ] = None  #  (string) : The code of the country region
    region_id: Optional[str] = None  #  (string) : The ID of the country region


class Country(MyBaseModel):
    """Country"""

    country_name: Optional[str] = None  #  (string) : The country name
    country_code: Optional[str] = None  #  (string) : The country code
    country_id: Optional[str] = None  #  (string) : The country ID
    regions: List[Region]  #  (Array[Region])


class TermBrief(MyBaseModel):
    """TermBrief"""

    term_id: Optional[str] = None  #  (string) : The term ID
    name: Optional[str] = None  #  (string) : The term name
    disabled: bool  #  (boolean) : Whether the term is disabled


class DeliveryZone(MyBaseModel):
    """DeliveryZone"""

    delivery_zone_id: Optional[str] = None  #  (string) : The delivery zone ID
    delivery_zone_name: Optional[
        str
    ] = None  #  (string) : The delivery zone name
    countries: List[Country]  #  (Array[Country])
    terms: List[TermBrief]  #  (Array[TermBrief])


class VoucheringPolicy(MyBaseModel):
    """VoucheringPolicy"""

    vouchering_policy_id: Optional[
        str
    ] = None  #  (string) : The vouchering policy ID
    vouchering_policy_billing_plan: Optional[
        str
    ] = None  #  (string) : The billing plan of the vouchering policy
    vouchering_policy_billing_plan_description: Optional[
        str
    ] = None  #  (string) : The description of the vouchering policy billing plan
    vouchering_policy_redemption_url: Optional[
        str
    ] = None  #  (string) : The vouchering policy redemption URL


class TermChangeOption(MyBaseModel):
    """TermChangeOption"""

    term_change_option_id: Optional[
        str
    ] = None  #  (string) : The ID of the term change option
    from_term_id: Optional[str] = None  #  (string) : The ID of the "From" term
    from_term_name: Optional[
        str
    ] = None  #  (string) : The name of the "From" term
    from_resource_id: Optional[
        str
    ] = None  #  (string) : The ID of the "From" resource
    from_resource_name: Optional[
        str
    ] = None  #  (string) : The name of the "From" resource
    from_billing_plan: Optional[
        str
    ] = None  #  (string) : The "From" billing plan
    to_term_id: Optional[str] = None  #  (string) : The ID of the "To" term
    to_term_name: Optional[str] = None  #  (string) : The name of the "To" term
    to_period_id: Optional[
        str
    ] = None  #  (string) : The ID of the "To" term period
    to_resource_id: Optional[
        str
    ] = None  #  (string) : The ID of the "To" resource
    to_resource_name: Optional[
        str
    ] = None  #  (string) : The name of the "To" resource
    to_billing_plan: Optional[str] = None  #  (string) : The "To" billing plan
    billing_timing: Optional[
        str
    ] = None  #  (string) : The billing timing(0: immediate term change;1: term change at the end of the current cycle;2: term change on the next sell date;3: term change at the end of the current period)
    immediate_access: Optional[
        bool
    ] = None  #  (boolean) : Whether the access begins immediately
    prorate_access: Optional[
        bool
    ] = None  #  (boolean) : Whether the Prorate billing amount function is enabled
    description: Optional[
        str
    ] = None  #  (string) : A description of the term change option; provided by the client
    include_trial: Optional[
        bool
    ] = None  #  (boolean) : Whether trial is enabled (not in use, always "FALSE")
    to_scheduled: Optional[
        bool
    ] = None  #  (boolean) : Whether the subscription is upgraded to a scheduled term
    from_scheduled: Optional[
        bool
    ] = None  #  (boolean) : Whether the subscription is upgraded from a scheduled term
    shared_account_count: Optional[
        int
    ] = None  #  (int32) : The count of allowed shared-subscription accounts
    collect_address: Optional[
        bool
    ] = None  #  (boolean) : Whether to collect an address for this term


class User(MyBaseModel):
    """User"""

    first_name: Optional[str] = None  #  (string) : The user's first name
    last_name: Optional[str] = None  #  (string) : The user's last name
    email: Optional[
        EmailStr
    ] = None  #  (string) : The user's email address (single)
    personal_name: Optional[
        str
    ] = None  #  (string) : The user's personal name. Name and surname ordered as per locale
    uid: Optional[str] = None  #  (string) : The user's ID
    image1: Optional[str] = None  #  (string) : The user's profile image
    create_date: Optional[int] = None  #  (date-time) : The user creation date
    reset_password_email_sent: Optional[
        bool
    ] = None  #  (boolean) : Whether a reset password email is sent
    custom_fields: Optional[List] = None  #  (array)
    last_visit: Optional[
        int
    ] = None  #  (date-time) : The date of the user's last visit
    last_login: Optional[int] = None  #  (date-time) : The last login stamp


class UserAddress(MyBaseModel):
    """UserAddress"""

    user_address_id: Optional[
        str
    ] = None  #  (string) : The public ID of the user address
    region: Optional[Region] = None  #  (Region)
    country: Optional[Country] = None  #  (Country)
    city: Optional[str] = None  #  (string) : The name of the city
    postal_code: Optional[str] = None  #  (string) : The user's postal code
    company_name: Optional[str] = None  #  (string) : company_name
    first_name: Optional[str] = None  #  (string) : The user's first name
    last_name: Optional[str] = None  #  (string) : The user's last name
    personal_name: Optional[
        str
    ] = None  #  (string) : The user's personal name. Name and surname ordered as per locale
    address1: Optional[str] = None  #  (string) : The user's first address
    address2: Optional[str] = None  #  (string) : The user's second address
    phone: Optional[str] = None  #  (string) : The user's phone
    additional_fields: Optional[
        str
    ] = None  #  (string) : The additional address fields (json)


class BillingPlanItem(MyBaseModel):
    """BillingPlanItem"""

    date: Optional[str] = None
    duration: Optional[str] = None
    billing: Optional[str] = None
    totalBilling: Optional[str] = None
    billingInfo: Optional[str] = None
    billingWithoutTax: Optional[str] = None
    pricelessBillingPre: Optional[str] = None
    price: Optional[str] = None
    priceValue: Optional[float] = None
    originalPrice: Optional[str] = None
    originalPriceValue: Optional[float] = None
    priceAndTax: Optional[float] = None
    currency: Optional[str] = None
    period: Optional[str] = None
    shortPeriod: Optional[str] = None
    billingPeriod: Optional[str] = None
    isFree: Optional[bool] = None
    cycles: Optional[int] = None
    isPayWhatYouWant: Optional[bool] = None


class Term(MyBaseModel):
    """term"""

    term_id: Optional[str] = None  #  (string) : The term ID
    aid: Optional[str] = None  #  (string) : The application ID
    resource: Optional[Resource] = None  #  (Resource) : The resource
    type: Optional[str] = None  #  (string) : The term type
    type_name: Optional[str] = None  #  (string) : The term type name
    name: Optional[str] = None  #  (string) : The term name
    description: Optional[
        str
    ] = None  #  (string) : The description of the term
    product_category: Optional[str] = None  #  (string) : The product category
    verify_on_renewal: Optional[
        bool
    ] = None  #  (boolean) : Whether the term should be verified before renewal (if "FALSE", this step is skipped)
    create_date: Optional[int] = None  #  (date-time) : The creation date
    update_date: Optional[int] = None  #  (date-time) : The update date
    term_billing_descriptor: Optional[
        str
    ] = None  #  (string) : The term billing descriptor
    payment_billing_plan: Optional[
        str
    ] = None  #  (string) : The billing plan for the term
    payment_billing_plan_description: Optional[
        str
    ] = None  #  (string) : The description of the term billing plan
    payment_billing_plan_table: Optional[
        List[BillingPlanItem]
    ] = None  #  (array)
    payment_allow_renew_days: Optional[
        int
    ] = None  #  (int32) : How many days in advance users user can renew
    payment_force_auto_renew: Optional[
        bool
    ] = None  #  (boolean) : Prevents users from disabling autorenewal (always "TRUE" for dynamic terms)
    payment_is_custom_price_available: Optional[
        bool
    ] = None  #  (boolean) : Whether users can pay more than term price
    payment_is_subscription: Optional[
        bool
    ] = None  #  (boolean) : Whether this term (payment or dynamic) is a subscription (unlike one-off)
    payment_has_free_trial: Optional[
        bool
    ] = None  #  (boolean) : Whether payment includes a free trial
    payment_new_customers_only: Optional[
        bool
    ] = None  #  (boolean) : Whether to show the term only to users having no dynamic or purchase conversions yet
    payment_trial_new_customers_only: Optional[
        bool
    ] = None  #  (boolean) : Whether to allow trial period only to users having no purchases yet
    payment_allow_promo_codes: Optional[
        bool
    ] = None  #  (boolean) : Whether to allow promo codes to be applied
    payment_renew_grace_period: Optional[
        int
    ] = None  #  (int32) : The number of days after expiration to still allow access to the resource
    payment_allow_gift: Optional[
        bool
    ] = None  #  (boolean) : Whether the term can be gifted
    payment_currency: Optional[
        str
    ] = None  #  (string) : The currency of the term
    currency_symbol: Optional[str] = None  #  (string) : The currency symbol
    payment_first_price: Optional[
        float
    ] = None  #  (double) : The first price of the term
    schedule: Optional[Schedule] = None  #  (Schedule)
    schedule_billing: Optional[str] = None  #  (string) : The schedule billing
    custom_require_user: Optional[
        bool
    ] = None  #  (boolean) : Whether a valid user is required to complete the term
    custom_default_access_period: Optional[
        int
    ] = None  #  (int32) : The default access period
    adview_vast_url: Optional[
        str
    ] = None  #  (string) : The VAST URL for adview_access_period (deprecated).
    adview_access_period: Optional[
        int
    ] = None  #  (int32) : The access duration (deprecated)
    registration_access_period: Optional[
        int
    ] = None  #  (int32) : The access duration (in seconds) for the registration term
    registration_grace_period: Optional[
        int
    ] = None  #  (int32) : How long (in seconds) after registration users can get access to the term
    external_api_id: Optional[
        str
    ] = None  #  (string) : The ID of the external API configuration
    external_api_name: Optional[
        str
    ] = None  #  (string) : The name of the external API configuration
    external_api_source: Optional[
        int
    ] = None  #  (int32) : The source of the external API configuration
    external_api_form_fields: Optional[
        List[ExternalAPIField]
    ] = None  #  (Array[ExternalAPIField])
    evt_verification_period: Optional[
        int
    ] = None  #  (int32) : The periodicity (in seconds) of checking the EVT subscription with the external service
    evt_fixed_time_access_period: Optional[
        int
    ] = None  #  (int32) : The period to grant access for (in days)
    evt_grace_period: Optional[
        int
    ] = None  #  (int32) : The External API grace period
    evt_itunes_bundle_id: Optional[
        str
    ] = None  #  (string) : iTunes's bundle ID
    evt_itunes_product_id: Optional[
        str
    ] = None  #  (string) : iTunes's product ID
    evt_google_play_product_id: Optional[
        str
    ] = None  #  (string) : Google Play's product ID
    evt_cds_product_id: Optional[str] = None  #  (string) : The CDS product ID.
    evt: Optional[
        List[str]
    ] = None  #  (Term) : The external verification term (similar to external service term).
    collect_address: Optional[
        bool
    ] = None  #  (boolean) : Whether to collect an address for this term
    delivery_zone: Optional[
        List[DeliveryZone]
    ] = None  #  (Array[DeliveryZone])
    default_country: Optional[Country] = None  #  (Country)
    vouchering_policy: Optional[
        VoucheringPolicy
    ] = None  #  (VoucheringPolicy) : The vouchering policy for the term (deprecated, replaced with Subscription gifting flow)
    billing_config: Optional[
        str
    ] = None  #  (string) : The type of billing config
    is_allowed_to_change_schedule_period_in_past: Optional[
        bool
    ] = None  #  (boolean) : Whether the term allows to change its schedule period created previously
    collect_shipping_address: Optional[
        bool
    ] = None  #  (boolean) : Whether to collect a shipping address for this gift term
    change_options: Optional[
        List[TermChangeOption]
    ] = None  #  (Array[TermChangeOption])
    shared_account_count: Optional[
        int
    ] = None  #  (int32) : The count of allowed shared-subscription accounts
    shared_redemption_url: Optional[
        str
    ] = None  #  (string) : The shared subscription redemption URL
    billing_configuration: Optional[
        str
    ] = None  #  (string) : A JSON value representing a list of the access periods with billing configurations (replaced with "payment_billing_plan(String)")
    show_full_billing_plan: Optional[
        bool
    ] = None  #  (boolean) : Show full billing plan on checkout for the dynamic term
    external_term_id: Optional[
        str
    ] = None  #  (string) : The ID of the term in the external system. Provided by the external system.
    external_product_ids: Optional[
        str
    ] = None  #  (string) : “External products" are entities of the external system accessed by users. If you enter multiple values (separated by a comma), Piano will create a standard resource for each product and also a bundled resource that will group them. Example: "digital_prod,print_sub_access,main_articles".
    subscription_management_url: Optional[
        str
    ] = None  #  (string) : A link to the external system where users can manage their subscriptions (similar to Piano’s MyAccount).
    custom_data: Optional[
        dict
    ] = None  #  (object) : The custom fields similar to those filled in PD on Linked term creation.
    allow_start_in_future: Optional[
        bool
    ] = None  #  (boolean) : Allow start in the future
    maximum_days_in_advance: Optional[
        int
    ] = None  #  (int32) : Maximum days in advance


class UserSubscriptionListItem(MyBaseModel):
    """UserSubscriptionListItem"""

    model_config = ConfigDict(extra="forbid")

    subscription_id: str  #  (string) : The user subscription ID
    auto_renew: Optional[
        bool
    ] = None  #  (boolean) : Whether auto renewal is enabled for the subscription
    next_bill_date: Optional[
        int
    ] = None  #  (date-time) : The next bill date of the subscription
    next_verificaition_date: Optional[
        int
    ] = None  #  (date-time) : The next verfication date of the subscription
    payment_method: Optional[
        str
    ] = None  #  (string) : The payment method of the subscription
    billing_plan: Optional[
        str
    ] = None  #  (string) : The billing plan of the subscription
    user_payment_info_id: Optional[
        str
    ] = None  #  (string) : The user payment info ID
    status: Optional[str] = None  #  (string) : The subscription status
    status_name: Optional[
        str
    ] = None  #  (string) : The name of the subscription status
    status_name_in_reports: Optional[
        str
    ] = None  #  (string) : The subscription status name in reports
    term: Term  #  (Term)
    resource: Optional[Resource] = None  #  (Resource) : The resource
    user: Optional[User] = None  #  () : The user
    start_date: Optional[int] = None  #  (date-time) : The start date.
    cancelable: Optional[
        bool
    ] = None  #  (boolean) : Whether this subscription can be cancelled; cancelling means thatthe access will not be prolonged and the current access will be revoked
    cancelable_and_refundadle: Optional[
        bool
    ] = None  #  (boolean) : Whether the subscription can be cancelled with the payment for the last period refunded. Cancelling means that the access will not be prolonged and the current access will be revoked
    user_address: Optional[UserAddress] = None  #  () : The user address entity
    psc_subscriber_number: Optional[
        str
    ] = None  #  (string) : The PSC subscriber number
    external_api_name: Optional[
        str
    ] = None  #  (string) : The name of the external API configuration
    conversion_result: Optional[
        str
    ] = None  #  (string) : The conversion result
    is_in_trial: Optional[
        bool
    ] = None  #  (boolean) : Whether the subscription is currently in trial period
    trial_period_end_date: Optional[
        int
    ] = None  #  (date-time) : The date when the trial period ends
    trial_amount: Optional[
        float
    ] = None  #  (double) : The price of the trial period
    trial_currency: Optional[
        str
    ] = None  #  (string) : The currency of the trial period
    end_date: Optional[int] = None  #  (date-time) : The subscription end date
    charge_count: Optional[
        int
    ] = None  #  (int32) : The user subscription charge count
    upi_ext_customer_id: Optional[
        str
    ] = None  #  (string) : The external customer ID of the payment method (user payment info)
    upi_ext_customer_id_label: Optional[
        str
    ] = None  #  (string) : The label of the external customer ID for the payment method (user payment info)
    shared_account_limit: Optional[
        int
    ] = None  #  (int32) : The shared account limit
    can_manage_shared_subscription: Optional[
        bool
    ] = None  #  (boolean) : Whether the shared subscription can be managed
    shared_accounts: Optional[
        List[UserSubscriptionAccount]
    ] = None  #  (Array[])
    cds_account_number: Optional[str] = None  #  (string) : CDS Account number
    external_sub_id: Optional[
        str
    ] = None  #  (string) : The ID of the external subscription linked with subscription.
    access_custom_data: Optional[
        str
    ] = None  #  (string) : Access custom data associated with linked term subscription
    marketing_id: Optional[str] = None
