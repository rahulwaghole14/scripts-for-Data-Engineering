{{
  config(
    tags=['sentiment']
  )
}}

WITH accounts AS (

    SELECT
    account.user_id
    , CASE
            WHEN lower(trim(account.country)) = 'new zealand' THEN 'NZ'
            WHEN lower(trim(account.country)) = 'united states' THEN 'US'
            WHEN lower(trim(account.country)) = 'australia' THEN 'AU'
            ELSE lower(trim(account.country))
        END AS country
    , account.postcode
        , CASE
            WHEN account.gender IN ('M', 'Male', 'Man') THEN 'M'
            WHEN account.gender IN ('F', 'Female', 'Woman') THEN 'F'
            WHEN account.gender IN ('O', 'Non-binary', 'Takatapui') THEN 'O'
            WHEN account.gender IN ('N', '-1') THEN 'N'
            WHEN account.gender IN ('P', 'Rather not say', 'Rather-Not-Say') THEN 'P'
            WHEN account.gender IS NULL OR gender = 'U' THEN NULL
            ELSE NULL
        END AS gender
    , account.city_region
    , account.locality
    , CASE
        WHEN account.date_of_birth = 'NaT' THEN NULL
        ELSE SAFE_CAST(account.date_of_birth AS DATE) END AS date_of_birth
    FROM {{ ref('stg_idm__user_profiles') }} account
    INNER JOIN {{ ref('sat_question_response') }} response ON account.user_id = response.RESPONSE_hexa_ACCOUNT_ID

)

SELECT accounts.user_id
    , ANY_VALUE(accounts.country) AS country
    , ANY_VALUE(accounts.postcode) AS postcode
    , ANY_VALUE(accounts.gender) AS gender
    , ANY_VALUE(accounts.city_region) AS city_region
    , ANY_VALUE(accounts.locality) AS locality
    , ANY_VALUE(accounts.date_of_birth) AS date_of_birth
FROM accounts GROUP BY 1
UNION ALL
SELECT -1 AS user_id
    , '(not set)' AS country
    , '(not set)' AS postcode
    , '(not set)' AS gender
    , '(not set)' AS city_region
    , '(not set)' AS locality
    , NULL AS date_of_birth
