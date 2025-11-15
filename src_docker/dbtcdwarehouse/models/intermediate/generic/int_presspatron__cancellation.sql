{{
  config(
    tags=['presspatron_api']
  )
}}

WITH subs AS (
  SELECT * FROM {{ ref('stg_presspatron__api_subscriptions') }}
),
users AS (
  SELECT * FROM {{ ref('stg_presspatron__api_users') }}
)

SELECT
  TIMESTAMP(u.created_at) AS SignUp_date,
  TIMESTAMP(s.cancellation_at) AS cancellation_at,
  s.frequency,
  s.subscription_status,
  u.email,
  u.first_name,
  u.last_name,
  u.newsletter_subscribed,
  TIMESTAMP(u.updated_at) AS updated_at,
  u.user_id
FROM subs s
JOIN users u ON u.user_id = s.user_id
WHERE (u.email is not null AND u.email <> '')
  AND (s.subscription_status <> 'active' AND s.subscription_status <> 'Past_due' AND s.cancellation_at <> '' AND  s.cancellation_at is not null)
