{{
  config(
    tags=['sentiment']
  )
}}

WITH regions AS (

    SELECT DISTINCT
        CAST(LOWER(COALESCE(RESPONSE_REGION_NAME, '(not set)')) AS STRING) REGION
        , RECORD_SOURCE
    FROM {{ ref('sat_question_response') }}
    WHERE RECORD_SOURCE = "QUALTRICS_hexa.CO.NZ"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY RESPONSE_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, add_id AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY REGION) AS REGION_ID
        , *
    FROM regions

)

SELECT * FROM add_id
