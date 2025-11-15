{{
  config(
        tags=['braze']
    )
}}

with source as (
      select * from {{ source('presspatron', 'presspatron__braze_user_profiles') }}
),
renamed as (
    select
        {{ adapter.quote("record_load_dts") }},
        {{ adapter.quote("sequence_nr") }},
        {{ adapter.quote("TransactionID") }},
        {{ adapter.quote("Sign_up_date") }},
        {{ adapter.quote("First_name") }},
        {{ adapter.quote("Last_name") }},
        {{ adapter.quote("Email_address") }},
        {{ adapter.quote("Currency") }},
        {{ adapter.quote("Subscribed_to_newsletter") }},
        {{ adapter.quote("Active_supporter") }},
        {{ adapter.quote("Frequency_of_recurring_payment") }},
        {{ adapter.quote("Recurring_contribution_amount") }},
        {{ adapter.quote("Total_contributions_to_date") }},
        {{ adapter.quote("record_source") }},
        {{ adapter.quote("hash_key") }},
        {{ adapter.quote("hash_diff") }},
        LOWER({{ adapter.quote("marketing_id") }}) AS marketing_id

    from source
)
select * from renamed
