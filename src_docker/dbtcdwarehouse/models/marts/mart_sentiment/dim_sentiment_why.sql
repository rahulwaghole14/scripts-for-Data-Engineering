{{
  config(
    tags=['sentiment']
  )
}}

WITH default_values AS (

    SELECT
        -1 AS SENTIMENT_WHY_ID
        , '(not set)' AS RESPONSE_KEY
        , '(not set)' AS RESPONSE_HASH
        , '(not set)' AS ANSWER

)

, data_values AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY hub_question_response.RESPONSE_HASH) AS SENTIMENT_WHY_ID
        , hub_question_response.RESPONSE_KEY AS RESPONSE_KEY
        , hub_question_response.RESPONSE_HASH
        , CASE
            WHEN sat_question_response.RESPONSE_VALUE IS NULL THEN '(not set)'
            ELSE LOWER(COALESCE(sat_question_response.RESPONSE_VALUE, ''))
        END AS ANSWER
    FROM {{ ref('hub_question_response') }} hub_question_response
    INNER JOIN {{ ref('sat_question_response') }} sat_question_response
        ON
            hub_question_response.RESPONSE_HASH = sat_question_response.RESPONSE_HASH
            AND sat_question_response.QUESTION_SURVEYQUESTIONID = 'SV_cNNywfC9H5SPVk2QID12'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sat_question_response.RESPONSE_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

SELECT * FROM default_values
UNION ALL
SELECT * FROM data_values
