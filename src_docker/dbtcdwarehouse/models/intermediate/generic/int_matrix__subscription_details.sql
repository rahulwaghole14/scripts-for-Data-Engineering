{{
config(
    tags='bq_matrix'
)
}}

WITH matrix_subscriptions AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscriptions') }}
)

, subord_cancel AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subord_cancel') }}
)

, matrix_subscription_details AS (
    SELECT
    DISTINCT
        matrix_subscriptions.subs_pointer
        , matrix_subscriptions.ServiceID
        , matrix_subscriptions.ProductID
        , matrix_subscriptions.period_id
        , matrix_subscriptions.sord_startdate
        , matrix_subscriptions.sord_stopdate
        , matrix_subscriptions.exp_end_date
        , matrix_subscriptions.paytype_id
        , matrix_subscriptions.camp_id
        , subord_cancel.canx_date
        , subord_cancel.canx_id
        , CASE
            WHEN
                (matrix_subscriptions.sord_stopdate >= CURRENT_DATE('Pacific/Auckland') OR matrix_subscriptions.sord_stopdate IS NULL)
                AND (subord_cancel.canx_date >= CURRENT_DATE('Pacific/Auckland') OR subord_cancel.canx_date IS NULL)
                AND (matrix_subscriptions.exp_end_date >= CURRENT_DATE('Pacific/Auckland') OR matrix_subscriptions.exp_end_date IS NULL)
                THEN 1
            ELSE 0
        END AS active_subscription
        , GREATEST(
            IFNULL(matrix_subscriptions.update_time, TIMESTAMP '1970-01-01'),
            IFNULL(subord_cancel.update_time, TIMESTAMP '1970-01-01')
        ) AS update_time
    FROM matrix_subscriptions
    LEFT JOIN subord_cancel
        ON
            matrix_subscriptions.sord_pointer = subord_cancel.sord_pointer
            AND matrix_subscriptions.ProductID = subord_cancel.ProductID
)

SELECT *
, GREATEST(
    IFNULL(sord_stopdate, DATE '1900-01-01'),
    IFNULL(exp_end_date, DATE '1900-01-01'),
    IFNULL(canx_date, DATE '1900-01-01')
    ) AS last_touch_date
FROM matrix_subscription_details
-- filter to relevant data for marketing
WHERE (canx_id = 'DECEASED' OR active_subscription = 1)
