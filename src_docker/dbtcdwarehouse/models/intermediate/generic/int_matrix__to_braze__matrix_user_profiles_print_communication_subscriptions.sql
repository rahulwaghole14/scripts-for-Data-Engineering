{{
  config(
        tags=['bq_matrix']
    )
}}

WITH data_tables AS (

    SELECT * FROM {{ ref('int_braze__generic_marketing_list') }}
    UNION ALL
    SELECT * FROM {{ ref('int_braze__magazine_marketing_list') }}

)

, grouped AS (

    SELECT marketing_id, internal_name, MAX(updated_at) AS updated_at
    FROM data_tables
    GROUP BY marketing_id, internal_name

)

, blacklist AS (

    SELECT
        LOWER(marketing_id) AS marketing_id,
        MAX(updated_at) AS updated_at
    FROM {{ ref('stg_acm__print_blacklist_final') }}
    GROUP BY LOWER(marketing_id)

)

, all_subs AS (

    SELECT DISTINCT marketing_id
        , 'generic_marketing_newsletter_print-subscriber-marketing' AS internal_name
        , DoNotCall as print__do_not_contact
        , updated_at
    FROM {{ ref('int_matrix__to_braze_user_profile') }}

)

, final AS (
    SELECT
        LOWER(marketing_id) AS marketing_id
        , '[' || ARRAY_TO_STRING(
        ARRAY_AGG(
            '{ "communication_reference" : "' || internal_name || '", "consent_status" : "subscribed" }'
            ORDER BY internal_name
        ),
        ','
        ) || ']' AS print_communication_subscriptions,
        MAX(updated_at) AS updated_at
    FROM
        grouped
    GROUP BY
        marketing_id

    UNION ALL

    SELECT DISTINCT lower(marketing_id) AS marketing_id
        , CASE WHEN print__do_not_contact = 1 THEN '[]' ELSE
          '{ "communication_reference" : "' || internal_name || '", "consent_status" : "subscribed" }'
        END AS print_communication_subscriptions
        , updated_at
    FROM all_subs
    WHERE marketing_id NOT IN (SELECT marketing_id FROM grouped)
    AND marketing_id NOT IN (SELECT marketing_id FROM blacklist)
)

, aggregated_final AS (
    SELECT
        marketing_id
        , '{ "print_communication_subscriptions" : ' ||
            ARRAY_TO_STRING(ARRAY_AGG(print_communication_subscriptions
            ORDER BY LENGTH(print_communication_subscriptions) DESC LIMIT 1), ',')
            || ' }' AS print_communication_subscriptions
        , MAX(updated_at) AS updated_at
    FROM final
    GROUP BY marketing_id
)

SELECT
    marketing_id
    , print_communication_subscriptions
    , updated_at
FROM aggregated_final
