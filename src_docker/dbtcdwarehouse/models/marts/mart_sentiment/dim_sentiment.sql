{{
  config(
    tags=['sentiment']
  )
}}

WITH default_values AS (

    SELECT
        -1 AS SENTIMENT_ID
        , '(not set)' AS RESPONSE_KEY
        , '(not set)' AS RESPONSE_HASH
        , '(not set)' AS ANSWER
        , '(not set)' AS RESPONSE_ID

)

, data_values AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY hub_question_response.RESPONSE_HASH) AS SENTIMENT_ID
        , hub_question_response.RESPONSE_KEY AS RESPONSE_KEY
        , hub_question_response.RESPONSE_HASH
        , CASE
            WHEN sat_question_response.RESPONSE_VALUE IS NULL THEN '(not set)'
            ELSE LOWER(COALESCE(sat_question_response.RESPONSE_VALUE, ''))
        END AS ANSWER
        , sat_question_response.RESPONSE_RESPONSEID AS RESPONSE_ID
    FROM {{ ref('hub_question_response') }} hub_question_response
    INNER JOIN {{ ref('sat_question_response') }} sat_question_response
        ON
            hub_question_response.RESPONSE_HASH = sat_question_response.RESPONSE_HASH
            AND sat_question_response.QUESTION_SURVEYQUESTIONID IN (
                'SV_cNNywfC9H5SPVk2QID1'
                , 'SV_cNNywfC9H5SPVk2Q5_1'
                , 'SV_cNNywfC9H5SPVk2Q5_2'
                , 'SV_cNNywfC9H5SPVk2Q5_3'
                , 'SV_cNNywfC9H5SPVk2Q5_4'
                , 'SV_cNNywfC9H5SPVk2Q5_5'
                , 'SV_cNNywfC9H5SPVk2Q5_6'
                , 'SV_cNNywfC9H5SPVk2Q5_7'
            )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sat_question_response.RESPONSE_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

SELECT * FROM default_values
UNION ALL
SELECT * FROM data_values
