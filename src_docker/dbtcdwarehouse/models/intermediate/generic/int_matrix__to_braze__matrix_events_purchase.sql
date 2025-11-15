{{
  config(
    tags='bq_matrix'
  )
}}

WITH subscribers AS (
    SELECT
        customers.hash_key AS primary_key,
        LOWER(customers.marketing_id) AS marketing_id,
        customers.subs_id AS print__sub_id
    FROM {{ ref('int_matrix__to_braze_user_profile') }} AS customers
),

purchased_subscriptions AS (
    SELECT
        subscribers.subs_id AS print__sub_id,
        subscriptions.period_id AS print_subscription_period,
        subscriptions.productid AS print_product_purchased,
        subscriptions.sord_startdate AS print_subscription_date,
        GREATEST(
            COALESCE(subscriptions.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(subord_cancel.update_time, TIMESTAMP '1970-01-01'),
            COALESCE(CAST(sord_entry_date AS TIMESTAMP), TIMESTAMP '1970-01-01')
        ) AS latest_timestamp,
        ROW_NUMBER() OVER (PARTITION BY subscribers.subs_id  ORDER BY subscriptions.sord_startdate DESC) AS rn
    FROM {{ ref('stg_matrix__bq_subscriptions') }} AS subscriptions
    LEFT JOIN {{ ref('stg_matrix__bq_subord_cancel') }} AS subord_cancel
        ON subscriptions.sord_pointer = subord_cancel.sord_pointer
        AND subscriptions.ProductID = subord_cancel.ProductID
    LEFT JOIN {{ ref('stg_matrix__bq_subscribers') }} AS subscribers
        ON subscriptions.subs_pointer = subscribers.subs_pointer
    WHERE
        (subscriptions.sord_stopdate >= CURRENT_DATE('Pacific/Auckland') OR subscriptions.sord_stopdate IS NULL)
        AND (subord_cancel.canx_date >= CURRENT_DATE('Pacific/Auckland') OR subord_cancel.canx_date IS NULL)
        AND (subscriptions.exp_end_date >= CURRENT_DATE('Pacific/Auckland') OR subscriptions.exp_end_date IS NULL)
        AND subscriptions.sord_startdate < CURRENT_DATE('Pacific/Auckland')
        AND DATE(subscriptions.sord_startdate) = DATE_SUB(CURRENT_DATE('Pacific/Auckland'), INTERVAL 1 DAY)
),

aggregated_purchased_subscriptions AS (
    SELECT
        print__sub_id,
        ARRAY_AGG(DISTINCT print_subscription_period) AS print_subscription_period,
        ARRAY_AGG(DISTINCT print_product_purchased) AS print_product_purchased,
        MAX(print_subscription_date) AS print_subscription_date,
        MAX(latest_timestamp) AS latest_timestamp
    FROM purchased_subscriptions
    GROUP BY print__sub_id

),

re_subscribers AS (
--This CTE identifies re_subscribers which are the subscribers that create new subscriptions within 20 days after stopped.
    SELECT DISTINCT
        s.subs_id
    FROM {{ ref('stg_matrix__bq_subscriptions') }} AS subscriptions
    LEFT JOIN {{ ref('stg_matrix__bq_subscribers') }} AS s
        ON subscriptions.subs_pointer = s.subs_pointer
    INNER JOIN purchased_subscriptions
        ON s.subs_id = purchased_subscriptions.print__sub_id and rn = 1
    WHERE subscriptions.ProductID = purchased_subscriptions.print_product_purchased
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.subs_id ORDER BY subscriptions.sord_stopdate DESC) = 1
      AND DATE_DIFF(purchased_subscriptions.print_subscription_date, subscriptions.sord_stopdate, DAY) <= 40
      AND purchased_subscriptions.print_subscription_date > subscriptions.sord_stopdate
    ORDER BY s.subs_id
)

SELECT
    subscribers.primary_key AS primary_key
    , subscribers.marketing_id AS external_id
    , 'print_purchase' AS event_name
    , latest_timestamp AS event_time
    , TO_JSON(
        STRUCT(
            purchased_subscriptions.print_subscription_date AS print_subscription_date,
            purchased_subscriptions.print_product_purchased AS print_product_purchased,
            purchased_subscriptions.print_subscription_period AS print_subscription_period
        )
    ) AS properties
FROM purchased_subscriptions
LEFT JOIN subscribers
    ON purchased_subscriptions.print__sub_id = subscribers.print__sub_id
LEFT JOIN re_subscribers
    ON purchased_subscriptions.print__sub_id = re_subscribers.subs_id
WHERE re_subscribers.subs_id IS NULL
ORDER BY subscribers.primary_key
