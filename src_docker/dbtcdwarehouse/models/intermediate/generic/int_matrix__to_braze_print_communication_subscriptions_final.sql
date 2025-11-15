{{
  config(
        tags=['bq_matrix']
    )
}}

WITH communication_subscriptions AS (
SELECT * FROM {{ ref('int_matrix__to_braze__matrix_user_profiles_print_communication_subscriptions') }}
)

SELECT
    marketing_id as external_id
     , PARSE_JSON(print_communication_subscriptions) as payload
     , updated_at
FROM communication_subscriptions