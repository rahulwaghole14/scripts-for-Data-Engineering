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
),
transactions AS (
  SELECT * FROM {{ ref('stg_presspatron__api_transactions') }}
),
JoinTables AS (
  SELECT
    CASE WHEN s.frequency IS NULL THEN t.created_at ELSE u.created_at END AS SignUp_date,
    u.updated_at,
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.newsletter_subscribed,
    t.gross_amount,
    t.card_issue_country,
    t.load_dts,
    CASE WHEN s.frequency IS NULL THEN 'one-time' ELSE s.frequency END AS frequency,
    CASE WHEN s.subscription_status IS NULL THEN 'inactive' ELSE s.subscription_status END AS subscription_status
  FROM users u
  LEFT JOIN subs s ON u.user_id = s.user_id
  LEFT JOIN transactions t ON u.user_id = t.user_id
  WHERE u.email IS NOT NULL AND TRIM(u.email) != '' AND COALESCE(s.subscription_status, '') != ''
),
rank AS (
  SELECT
    user_id,
    first_name,
    last_name,
    email,
    SignUp_date,
    frequency,
    newsletter_subscribed,
    subscription_status,
    gross_amount,
    card_issue_country,
    load_dts,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY SignUp_date DESC
    ) AS rn
  FROM
    JoinTables
)
SELECT
  user_id,
  first_name,
  last_name,
  email,
  SignUp_date,
  frequency,
  newsletter_subscribed AS last_subscribed_to_newsletter,
  subscription_status AS last_active_supporter,
  gross_amount AS last_amount_of_transaction,
  card_issue_country AS last_currency
FROM
  rank
WHERE rn = 1
