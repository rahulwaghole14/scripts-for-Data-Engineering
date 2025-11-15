with source as (
      select * from {{ source('piano', 'piano__subscriptions') }}
),
renamed as (
    select
        {{ adapter.quote("subscription_id") }},
        {{ adapter.quote("auto_renew") }},
        {{ adapter.quote("next_bill_date") }},
        {{ adapter.quote("next_verificaition_date") }},
        {{ adapter.quote("payment_method") }},
        {{ adapter.quote("billing_plan") }},
        {{ adapter.quote("user_payment_info_id") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("status_name") }},
        {{ adapter.quote("status_name_in_reports") }},
        {{ adapter.quote("term") }},
        {{ adapter.quote("resource") }},
        {{ adapter.quote("user") }},
        {{ adapter.quote("start_date") }},
        {{ adapter.quote("cancelable") }},
        {{ adapter.quote("cancelable_and_refundadle") }},
        {{ adapter.quote("user_address") }},
        {{ adapter.quote("psc_subscriber_number") }},
        {{ adapter.quote("external_api_name") }},
        {{ adapter.quote("conversion_result") }},
        {{ adapter.quote("is_in_trial") }},
        {{ adapter.quote("trial_period_end_date") }},
        {{ adapter.quote("trial_amount") }},
        {{ adapter.quote("trial_currency") }},
        {{ adapter.quote("end_date") }},
        {{ adapter.quote("charge_count") }},
        {{ adapter.quote("upi_ext_customer_id") }},
        {{ adapter.quote("upi_ext_customer_id_label") }},
        {{ adapter.quote("shared_account_limit") }},
        {{ adapter.quote("can_manage_shared_subscription") }},
        {{ adapter.quote("shared_accounts") }},
        {{ adapter.quote("cds_account_number") }},
        {{ adapter.quote("external_sub_id") }},
        {{ adapter.quote("access_custom_data") }},
        {{ adapter.quote("marketing_id") }}

    from source
)
select * from renamed
