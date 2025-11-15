{{
  config(
    tags='bq_matrix'
  )
}}


WITH subscribers AS (
    SELECT
        customers.hash_key AS primary_key
        , LOWER(customers.marketing_id) AS marketing_id
        , customers.subs_id AS print__sub_id
    FROM {{ ref('int_matrix__to_braze_user_profile') }} AS customers
)

, complaints AS (
    SELECT
        sord_pointer
        , comp_date AS print_complaint_date
        , ProductID AS print_complaint_product
        , comp_type AS print_complaint_type
        , GREATEST(
            COALESCE(update_time, TIMESTAMP '1970-01-01'),
            COALESCE(TIMESTAMP_SECONDS(timestamp), TIMESTAMP '1970-01-01'),
            COALESCE(resolved_datetime, TIMESTAMP '1970-01-01'),
            COALESCE(CAST(comp_entdate AS TIMESTAMP), TIMESTAMP '1970-01-01'),
            COALESCE(CAST(comp_date AS TIMESTAMP), TIMESTAMP '1970-01-01')
        ) AS latest_timestamp
    FROM {{ ref('stg_bq_matrix_complaints') }}
    WHERE CAST(comp_date AS DATE) = CAST(DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -1 DAY) AS DATE)
)

, subscriptions AS (
    SELECT
    DISTINCT
        subscriptions.sord_pointer
        , subscriber.subs_id
    FROM {{ ref('stg_matrix__bq_subscriptions') }} AS subscriptions
    LEFT JOIN {{ ref('stg_matrix__bq_subscribers') }} AS subscriber
        ON subscriptions.subs_pointer = subscriber.subs_pointer
)

SELECT
    subscriptions.subs_id
    , MAX(subscribers.primary_key) AS primary_key
    , MAX(subscribers.marketing_id) AS external_id
    , 'print_complaints' AS event_name
    , MAX(complaints.latest_timestamp) AS event_time
    , TO_JSON(
        STRUCT(
            MAX(complaints.print_complaint_date) AS print_complaint_date,
            ARRAY_AGG(DISTINCT TRIM(complaints.print_complaint_type)) AS print_complaint_type,
            ARRAY_AGG(DISTINCT TRIM(complaints.print_complaint_product)) AS print_complaint_product
            )
      ) AS properties
FROM complaints
LEFT JOIN subscriptions ON complaints.sord_pointer = subscriptions.sord_pointer
LEFT JOIN subscribers ON subscriptions.subs_id = subscribers.print__sub_id
GROUP BY subscriptions.subs_id
