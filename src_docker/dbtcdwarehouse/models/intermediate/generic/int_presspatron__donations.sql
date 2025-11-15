{{
  config(
        tags=['presspatron_api']
    )
}}

WITH v_transactions AS (
    SELECT *
    FROM {{ ref('stg_presspatron__api_transactions') }}
),
v_users AS (
    SELECT *
    FROM {{ ref('stg_presspatron__api_users') }}
)

SELECT
    TIMESTAMP(t.created_at) AS transaction_date,
    t.transaction_id,
    u.user_id,
    t.gross_amount AS amount_of_transaction,
    t.frequency,
    t.payment_status,
    TIMESTAMP(t.load_dts) AS load_dts,
    TIMESTAMP(u.created_at) AS SignUp_date,
    u.first_name,
    u.last_name,
    u.email AS email_address,
    CASE
        WHEN u.email = '' THEN 'N'
        WHEN t.frequency = 'one-time' THEN 'Y'
        WHEN ROW_NUMBER() OVER (
            PARTITION BY t.frequency, t.payment_status, u.created_at, u.email
            ORDER BY u.email ASC, t.frequency ASC, u.created_at ASC, t.created_at ASC
        ) = 1 THEN 'Y'
        ELSE 'N'
    END AS EmailYN,
    CURRENT_TIMESTAMP() as upload_timestamp
FROM v_transactions t
JOIN v_users u ON u.user_id = t.user_id
WHERE NOT (t.payment_status = 'failed' AND u.email = '')
      AND t.payment_status <> 'refunded'
      AND t.gross_amount <> '0'
ORDER BY t.created_at ASC
