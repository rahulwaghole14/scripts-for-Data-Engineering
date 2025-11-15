{{
  config(
    tags='bq_matrix'
  )
}}

WITH subscribers AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscribers') }}
)

, subscriptions AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscriptions') }}
)

, customers AS (
    SELECT * FROM {{ ref('int_matrix__customers') }}
)

, subord_cancel AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subord_cancel') }}
)

SELECT
DISTINCT
    subscriptions.ProductID AS product_id
    , period_id
    , ServiceID AS service_id
    , subscribers.subs_id AS subscriber_id
    , sord_id AS subscription_id
    , AddrLine5 AS suburb
    , AddrLine6 AS town_city
    , country_name AS country
    , sord_entry_date AS entry_date
    , sord_startdate AS start_date
    , sord_stopdate AS stop_date
    , subord_cancel.request_date AS request_date
    , subord_cancel.canx_date AS cancellation_date
    , subord_cancel.canx_id AS cancellation_id
    , sponsor_ref
    , subscriptions.subs_pointer
    , subscriptions.sord_pointer
    , order_type
    , exp_end_date AS expected_renewal_date
    , last_invoiced
    , dist_type
    , camp_id
    , paidthrudate AS paid_thru_date
    , CurPeriod AS cur_period
    , cost_centre
    , init_grace_issues
    , invoice_flag
    , paytype_id
    , subscriptions.SourceID
    , sysperson_person
    , 'MATRIX' AS record_source
FROM subscriptions
INNER JOIN subscribers
    ON subscriptions.subs_pointer = subscribers.subs_pointer
INNER JOIN customers
    ON subscribers.subs_id = customers.subs_id
LEFT JOIN subord_cancel
    ON subscriptions.sord_pointer = subord_cancel.sord_pointer
