{{
  config(
    tags=['presspatron_api'],
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
  )
}}

WITH subs AS (
    SELECT * FROM {{ ref('stg_presspatron__api_subscriptions') }}
)

, users AS (
    SELECT * FROM {{ ref('stg_presspatron__api_users') }}
)

, transactions AS (
    SELECT * FROM {{ ref('stg_presspatron__api_transactions') }}
    WHERE payment_status = 'successful'
)

, JoinTables AS (
    SELECT
         t.sign_up_date AS SignUp_date
        ,t.created_at  AS transaction_created_date
        , u.updated_at
        , u.user_id
        , u.first_name
        , u.last_name
        , u.email
        , u.newsletter_subscribed
        , CAST(t.gross_amount AS numeric) AS gross_amount
        , t.card_issue_country
        , GREATEST(
            COALESCE(t.load_dts, '1900-01-01')
            , COALESCE(s.load_dts, '1900-01-01')
            , COALESCE(u.load_dts, '1900-01-01')
        )
            AS max_load_dts   -- Taking the max load_dts
        , CASE WHEN s.frequency IS NULL THEN 'one-time' ELSE s.frequency END AS frequency
        , CASE WHEN s.subscription_status IS NULL THEN 'inactive' ELSE s.subscription_status END AS subscription_status
    FROM users u
    LEFT JOIN subs s ON u.user_id = s.user_id
    LEFT JOIN transactions t ON u.user_id = t.user_id
    WHERE u.email IS NOT NULL AND TRIM(u.email) != ''
)

, TotalContribution AS (
    SELECT
        user_id
        , SUM(gross_amount) AS total_contribution
    FROM JoinTables
    GROUP BY
        user_id
)

, Ranked AS (
    SELECT
        user_id
        , first_name
        , last_name
        , email
        , LOWER(TRIM(email)) AS base_id
        , SignUp_date
        , frequency
        , newsletter_subscribed
        , subscription_status
        , gross_amount
        , card_issue_country
        , max_load_dts
        , ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY max_load_dts DESC, transaction_created_date DESC
        ) AS rn
    FROM
        JoinTables
)

SELECT
    {{ format_uuid(generate_uuid_v5("base_id")) }}  AS marketing_id
    , r.user_id
    , r.first_name
    , r.last_name
    , r.email
    , CAST(SUBSTR(r.SignUp_date, 1, 10) AS DATE) AS SignUp_date
    , CASE
        WHEN r.newsletter_subscribed = 'True' THEN 1
        WHEN r.newsletter_subscribed = 'False' THEN 0
        ELSE NULL
    END AS subscribed_to_newsletter
    , CASE
        WHEN r.subscription_status = 'active' THEN 1
        ELSE 0
    END AS supporter__active
    , t.total_contribution
    , CASE WHEN r.subscription_status <> 'active' THEN 0 ELSE r.gross_amount END AS recurring_contribution
    , r.frequency
    , r.max_load_dts AS load_dts

FROM
    Ranked r
LEFT JOIN TotalContribution t
    ON r.user_id = t.user_id
WHERE
    rn = 1
    {% if is_incremental() %}
        -- This condition ensures that only new or updated records are processed based on the max load_dts
        AND r.max_load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {% endif %}
