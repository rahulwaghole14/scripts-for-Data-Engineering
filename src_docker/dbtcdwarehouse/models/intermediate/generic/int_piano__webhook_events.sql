{{
  config(
        tags=['piano']
    )
}}

WITH RAW AS (

  SELECT * FROM {{ ref('stg_piano__webhook_events') }}

)

, RENAME AS (

    SELECT
        JSON_EXTRACT_SCALAR(DATA, '$.access_id') AS ACCESS_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.aid') AS AID
        , JSON_EXTRACT_SCALAR(DATA, '$.uid') AS UID
        , JSON_EXTRACT_SCALAR(DATA, '$.subscription_id') AS SUBSCRIPTION_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.term_id') AS TERM_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.upi_id') AS UPI_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.contract_id') AS CONTRACT_ID
        , LOWER(TRIM(JSON_EXTRACT_SCALAR(DATA, '$.user_email'))) AS USER_EMAIL
        , JSON_EXTRACT_SCALAR(DATA, '$.start_date') AS START_DATE
        , JSON_EXTRACT_SCALAR(DATA, '$.expires') AS EXPIRES
        , JSON_EXTRACT_SCALAR(DATA, '$.event') AS EVENT
        , JSON_EXTRACT_SCALAR(DATA, '$.type') AS TYPE
        , JSON_EXTRACT_SCALAR(DATA, '$.term_type') AS TERM_TYPE
        , JSON_EXTRACT_SCALAR(DATA, '$.auto_renew') AS AUTO_RENEW
        , JSON_EXTRACT_SCALAR(DATA, '$.billing_plan') AS BILLING_PLAN
        , JSON_EXTRACT_SCALAR(DATA, '$.decline_reason') AS DECLINE_REASON
        , JSON_EXTRACT_SCALAR(DATA, '$.failure_counter') AS FAILURE_COUNTER
        , JSON_EXTRACT_SCALAR(DATA, '$.grace_period_length') AS GRACE_PERIOD_LENGTH
        , JSON_EXTRACT_SCALAR(DATA, '$.grace_period_start_date') AS GRACE_PERIOD_START_DATE
        , JSON_EXTRACT_SCALAR(DATA, '$.is_in_grace') AS IS_IN_GRACE
        , JSON_EXTRACT_SCALAR(DATA, '$.next_bill_date') AS NEXT_BILL_DATE
        , JSON_EXTRACT_SCALAR(DATA, '$.passive_churn_logic_id') AS PASSIVE_CHURN_LOGIC_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.payment_id') AS PAYMENT_ID
        , JSON_EXTRACT_SCALAR(DATA, '$.renewed_by') AS RENEWED_BY
        , JSON_EXTRACT_SCALAR(DATA, '$.rid') AS RID
        , JSON_EXTRACT_SCALAR(DATA, '$.status') AS STATUS
        , JSON_EXTRACT_SCALAR(DATA, '$.version') AS VERSION
        , RECORD_SOURCE
        , LOAD_DATETIMESTAMP
        , DATA
    FROM RAW

)

SELECT *
    , CASE WHEN AID = 'go7g2STDpa' THEN 'The Press'
    WHEN AID = '0V1Vwkflpa' THEN 'Waikato Times'
    WHEN AID = 'tISrUfqypa' THEN 'The Post'
    ELSE '(not set)' END AS MASTHEAD
FROM RENAME
