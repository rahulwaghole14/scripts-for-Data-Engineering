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

,cancelled_data AS (
    SELECT 
        subscribers.subs_id AS print__sub_id,
        subscriptions.period_id,
        subord_cancel.ProductID,
        subord_cancel.canx_id,
        subord_cancel.canx_date,
        subscriptions.sord_stopdate,
        GREATEST(
            COALESCE(subord_cancel.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(request_date, TIMESTAMP '1970-01-01'),
            COALESCE(CAST(canx_date AS TIMESTAMP), TIMESTAMP '1970-01-01')
        ) AS latest_timestamp,
        ROW_NUMBER() OVER (PARTITION BY subscribers.subs_id  ORDER BY canx_date DESC) AS rn
    FROM 
        {{ ref('stg_matrix__bq_subscriptions') }}  AS subscriptions
    LEFT JOIN 
        {{ ref('stg_matrix__bq_subord_cancel') }} AS subord_cancel
        ON subscriptions.sord_pointer = subord_cancel.sord_pointer
        AND subscriptions.ProductID = subord_cancel.ProductID
    LEFT JOIN 
        {{ ref('stg_matrix__bq_subscribers') }}  AS subscribers
        ON subscriptions.subs_pointer = subscribers.subs_pointer
    WHERE 
        (subscriptions.sord_stopdate < CURRENT_DATE('Pacific/Auckland') OR subscriptions.sord_stopdate IS NOT NULL)
        AND (subord_cancel.canx_date < CURRENT_DATE('Pacific/Auckland') OR subord_cancel.canx_date IS NOT NULL)
        AND (subscriptions.exp_end_date < CURRENT_DATE('Pacific/Auckland') OR subscriptions.exp_end_date IS NOT NULL)
        AND CAST(subord_cancel.canx_date AS DATE) = CAST(DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -1 DAY) AS DATE)
)

, aggregated_cancelled_data AS (
    SELECT
        print__sub_id,
        ARRAY_AGG(DISTINCT period_id) AS print_subscription_period,
        ARRAY_AGG(DISTINCT ProductID) AS print_cancellation_product_id,
        ARRAY_AGG(DISTINCT canx_id) AS print_cancellation_code,
        MAX(canx_date) AS print_cancellation_date,
        MAX(latest_timestamp) AS latest_timestamp
    FROM
        cancelled_data
    GROUP BY
    print__sub_id
)

,re_subscribers AS (
    SELECT 
    DISTINCT 
    s.subs_id
    FROM {{ ref('stg_matrix__bq_subscriptions') }}  AS subscriptions
    LEFT JOIN {{ ref('stg_matrix__bq_subscribers') }} AS s
        ON subscriptions.subs_pointer = s.subs_pointer
    INNER JOIN cancelled_data 
            ON s.subs_id = cancelled_data.print__sub_id AND rn = 1 
    WHERE subscriptions.ProductID = cancelled_Data.ProductID         
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.subs_id ORDER BY subscriptions.sord_startdate DESC) = 1
        AND DATE_DIFF(subscriptions.sord_startdate, cancelled_data.canx_date,  DAY) <= 40
        AND  cancelled_data.canx_date <= subscriptions.sord_startdate 
        AND  subscriptions.sord_stopdate IS NULL
    ORDER BY s.subs_id

)

SELECT
    subscribers.primary_key AS primary_key
    , subscribers.marketing_id AS external_id
    , 'print_cancellation' AS event_name
    , aggregated_cancelled_data.latest_timestamp AS event_time
    , TO_JSON(
        STRUCT(
            aggregated_cancelled_data.print_cancellation_date AS print_cancellation_date,
            aggregated_cancelled_data.print_cancellation_code AS print_cancellation_code,
            aggregated_cancelled_data.print_cancellation_product_id AS print_cancellation_product_id,
            aggregated_cancelled_data.print_subscription_period AS print_subscription_period
            )
      ) AS properties
FROM aggregated_cancelled_data
LEFT JOIN subscribers ON aggregated_cancelled_data.print__sub_id = subscribers.print__sub_id
LEFT JOIN re_subscribers ON re_subscribers.subs_id = aggregated_cancelled_data.print__sub_id
WHERE  re_subscribers.subs_id IS NULL


