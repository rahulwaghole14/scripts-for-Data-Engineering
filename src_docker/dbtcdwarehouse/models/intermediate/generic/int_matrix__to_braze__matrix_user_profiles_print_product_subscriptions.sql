{{
  config(
        tags=['bq_matrix']
    )
}}

WITH matrix_subscription_details AS (

    SELECT 
        DISTINCT 
        subs_pointer
        ,ServiceID
        ,ProductID
        ,period_id
        ,paytype_id
        ,camp_id
        ,exp_end_date
        ,sord_startdate 
    FROM {{ ref('int_matrix__subscription_details') }}

)

, round_id AS (

    SELECT
        DISTINCT
        subs_id 
        , Rate_head_id AS print__rate_id 
        , round_id AS print__round_id
        , product_id
    FROM {{ ref('stg_matrix__bq_rpt_subscribers_rate_rounds') }}
)

, marketing_id AS (

    SELECT
        DISTINCT 
        LOWER(marketing_id) AS marketing_id
        ,subs_id
    FROM {{ ref('int_matrix__to_braze_user_profile') }}
    WHERE marketing_id IS NOT NULL

)
, subs_id AS (
    SELECT 
        DISTINCT 
        subs_id
        ,subs_pointer
    FROM {{ ref('stg_matrix__bq_subscribers') }}
)



, joined_data AS (

    SELECT 
        DISTINCT 
        marketing_id.marketing_id
        , matrix_subscription_details.subs_pointer
        , matrix_subscription_details.ServiceID AS print__service_id
        , matrix_subscription_details.ProductID AS print__product_id
        , matrix_subscription_details.paytype_id AS print__payment_type
        , matrix_subscription_details.camp_id AS print__campaign_code
        , matrix_subscription_details.period_id AS print__sub_period
        , CAST(matrix_subscription_details.exp_end_date AS DATE) AS print__renewal_date
        , CAST(matrix_subscription_details.sord_startdate  AS DATE) AS print__start_date
        , round_id.print__rate_id
        , round_id.print__round_id

    FROM marketing_id
    LEFT JOIN subs_id
        ON subs_id.subs_id = marketing_id.subs_id
    LEFT JOIN matrix_subscription_details
        ON matrix_subscription_details.subs_pointer = subs_id.subs_pointer
    LEFT JOIN round_id
        ON round_id.subs_id = subs_id.subs_id and round_id.product_id = matrix_subscription_details.ProductID
    
)

, final AS (
    SELECT
        LOWER(marketing_id) AS marketing_id
        , '[' || ARRAY_TO_STRING(
        ARRAY_AGG(
            '{"print__product_id":"' || print__product_id || '","print__rate_id":"' || print__rate_id || ' ","print__service_id":"' || print__service_id || '","print__round_id":"' || print__round_id || '","print__sub_period":"' || print__sub_period || '","print__payment_type":"' || print__payment_type ||'","print__campaign_code":"' || print__campaign_code ||'","print__renewal_date":"' || print__renewal_date ||'","print__start_date":"' || print__start_date || '"}'
            ORDER BY print__product_id
        ),
        ','
        ) || ']' AS print_product_subscription
    FROM
        joined_data
    GROUP BY
        marketing_id

)

, aggregated_final AS (
    SELECT
        marketing_id
        , ARRAY_TO_STRING(ARRAY_AGG(print_product_subscription ORDER BY LENGTH(print_product_subscription) DESC LIMIT 1), ',') AS print_product_subscription
    FROM final
    GROUP BY marketing_id
)

SELECT
    *
    , TO_BASE64(MD5(TO_JSON_STRING(STRUCT(
        marketing_id
        , print_product_subscription
    )))) AS row_hash
FROM aggregated_final
