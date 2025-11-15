{{
  config(
    tags='bq_matrix'
  )
}}

WITH winback_subscribers AS (
    SELECT
        sub_id
        , CAST(proposed_rate AS STRING) AS proposed_rate
        , product_id
        , rate_id
        , cancellation_date
        , week_no
        , cancelled_pubs
        , cancelled_proposed_rate
        , cancelled_multiple_pubs
        , cancellation_id
        , sord_startdate
        , service
    FROM {{ ref('stg_matrix__bq_ffx_temp_winback_subscribers1') }}
)

, grouped_subscribers AS (
    SELECT
        sub_id AS print__sub_id
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT proposed_rate), ',') AS proposed_rate
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(product_id)), ',') AS product_id
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(rate_id)), ',') AS rate_id
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(week_no)), ',') AS week_no
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(cancelled_pubs)), ',') AS cancelled_pubs
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(cancelled_proposed_rate)), ',') AS cancelled_proposed_rate
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(cancellation_id)), ',') AS cancellation_id
        , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TRIM(service)), ',') AS service
        , MAX(CASE WHEN Cancelled_Multiple_Pubs = 'YES' THEN 1 ELSE 0 END) AS cancelled_multiple_pubs
        , MAX(sord_startdate) AS sord_startdate
        , MAX(cancellation_date) AS cancellation_date
    FROM winback_subscribers
    GROUP BY sub_id
)

, final_output AS (
    SELECT
        LOWER(customers.marketing_id) AS marketing_id
        , grouped_subscribers.proposed_rate AS print__winback_proposed_rate
        , grouped_subscribers.product_id AS print__winback_product_id
        , grouped_subscribers.rate_id AS print__winback_rate_id
        , FORMAT_TIMESTAMP('%Y-%m-%d', CAST(grouped_subscribers.Cancellation_Date AS TIMESTAMP)) AS print__winback_cancellation_date
        , grouped_subscribers.week_no AS print__winback_week
        , grouped_subscribers.cancelled_pubs AS print__winback_cancelled_pubs
        , grouped_subscribers.cancelled_proposed_rate AS print__winback_cancelled_proposed_rate
        , grouped_subscribers.cancelled_multiple_pubs AS print__cancelled_multiple_pubs
        , grouped_subscribers.cancellation_id AS print__winback_cancellation_id
        , grouped_subscribers.sord_startdate AS print__winback_start_date
        , grouped_subscribers.service AS print__winback_service
        , CURRENT_TIMESTAMP() AS timestamp_dts
    FROM grouped_subscribers
    LEFT JOIN {{ ref('int_matrix__to_braze_user_profile') }} customers
        ON TRIM(grouped_subscribers.print__sub_id) = customers.subs_id
)

SELECT
    marketing_id AS external_id
    , 'print_winback' AS event_name
    , timestamp_dts AS event_time
    , TO_JSON(
            STRUCT(
                print__winback_proposed_rate AS print__winback_proposed_rate,
                print__winback_product_id AS print__winback_product_id,
                print__winback_rate_id AS print__winback_rate_id,
                print__winback_cancellation_date AS print__winback_cancellation_date,
                print__winback_week AS print__winback_week,
                print__winback_cancelled_pubs AS print__winback_cancelled_pubs,
                print__winback_cancelled_proposed_rate AS print__winback_cancelled_proposed_rate,
                print__cancelled_multiple_pubs AS print__cancelled_multiple_pubs,
                print__winback_cancellation_id AS print__winback_cancellation_id,
                print__winback_start_date AS print__winback_start_date,
                print__winback_service AS print__winback_service
                )
    ) AS properties
FROM final_output
