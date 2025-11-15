{{
  config(
        tags=['piano']
    )
}}

WITH pianosubs AS (

    SELECT
        subscription_id AS sub__subscription_id
        , auto_renew AS sub__auto_renew
        , next_bill_date AS sub__next_bill_date
        , upi_ext_customer_id_label AS sub__upi_ext_customer_id
        , cancelable AS sub__cancelable
        , cancelable_and_refundadle AS sub__cancelable_and_refundadle
        , status_name AS sub__status_name
        , start_date AS sub__start_date
        , DATETIME(TIMESTAMP_SECONDS(start_date), "Pacific/Auckland") AS sub__start_date_nzt
        , is_in_trial AS sub__is_in_trial
        , trial_period_end_date AS sub__trial_period_end_date
        , trial_amount AS sub__trial_amount
        , charge_count AS sub__charge_count
        , user.uid AS user__uid
        , user.create_date AS user__create_date
        , term.aid AS term__aid
        , term.term_id AS term__term_id
        , term.name AS term__name
        , term.payment_first_price AS term__payment_first_price
        , term.payment_is_subscription AS term__payment_is_subscription
        , term.billing_config AS term__billing_config
        {# term.resource.aid AS term__resource_aid, #}
        , term.resource.rid AS term__resource_rid
        , term.resource.name AS term__resource_name
        , term.resource.type_label AS term__resource_type_label
        , term.resource.bundle_type_label AS term__resource_bundle_type_label
        , CASE WHEN term.resource.aid = "go7g2STDpa" THEN "The Press"
            WHEN term.resource.aid = "tISrUfqypa" THEN "The Post"
            WHEN term.resource.aid = "0V1Vwkflpa" THEN "Waikato Times"
            ELSE "(not set)" END AS piano__app
        , end_date AS sub__end_date

    FROM {{ ref('stg_piano__subscriptions') }}
)

SELECT * FROM pianosubs
