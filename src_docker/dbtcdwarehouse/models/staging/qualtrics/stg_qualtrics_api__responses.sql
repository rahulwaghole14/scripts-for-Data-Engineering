WITH source AS (

    SELECT * FROM {{ source('qualtrics', 'qualtrics__responses') }}

)

, region_polygons AS (

    SELECT * FROM {{ ref('stg_statsnz__region_polygons') }}

)

, renamed AS (

    SELECT
        responseId AS RESPONSE_RESPONSEID
        , 'SV_cNNywfC9H5SPVk2' AS RESPONSE_SURVEYID
        , CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', values_recordedDate) AS DATETIME) AS RESPONSE_RECORDEDDATE
        , PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_startDate) AS RESPONSE_STARTDATE
        , PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_endDate) AS RESPONSE_ENDDATE
        , CAST(values_status AS STRING) AS RESPONSE_STATUS
        , values_ipAddress AS RESPONSE_IPADDRESS
        , CAST(values_duration AS INT64) AS RESPONSE_DURATION
        , values_locationLatitude AS RESPONSE_LOCATIONLATITUDE
        , values_locationLongitude AS RESPONSE_LOCATIONLONGITUDE
        , B.region AS RESPONSE_REGION_NAME
        , values_distributionChannel AS RESPONSE_DISTRIBUTIONCHANNEL
        , values_pageType AS RESPONSE_PAGETYPE
        , values_sysEnv AS RESPONSE_SYSENV
        , values_pageReferrer AS RESPONSE_PAGEREFERRER
        , values_siteReferrer AS RESPONSE_SITEREFERRER
        , values_currentPageURL AS RESPONSE_CURRENTPAGEURL
        , values_QID12_TEXT AS RESPONSE_VALUE_QID12
        , JSON_EXTRACT_ARRAY(
            CASE
                WHEN values_QID1 IS NOT NULL THEN CONCAT('["', CAST(values_QID1 AS STRING), '"]')
                ELSE values_QID13
            END
        ) AS RESPONSE_VALUE_CODE
        , JSON_EXTRACT_ARRAY(
            REPLACE(
                CASE
                    WHEN JSON_QUERY(labels_QID1, '$') IS NOT NULL THEN labels_QID1
                    WHEN labels_QID1 IS NOT NULL THEN CONCAT('["', labels_QID1, '"]')
                    ELSE labels_QID13
                END
                , '\n', ''
            )
        ) AS RESPONSE_VALUE_2
        , CAST(REGEXP_EXTRACT(values_currentPageURL, r'(\d{6,})') AS INT64) AS ARTICLE_KEY
        , COALESCE(
            SAFE_CAST(values_memberID AS INT64)
            , SAFE_CAST(values_userID AS INT64)
            , NULL
        ) AS RESPONSE_hexa_ACCOUNT_ID
        , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', values_recordedDate) AS RECORD_LOAD_DTS
        , CURRENT_TIMESTAMP() AS EFFECTIVE_FROM
    FROM source
    LEFT JOIN region_polygons B ON ST_CONTAINS(
        B.region_polygon
        , ST_GEOGPOINT(
            CAST(values_locationLongitude AS FLOAT64)
            , CAST(values_locationLatitude AS FLOAT64)
        )
    )

)

, unnested AS (

    SELECT
        *
        , CONCAT(
            'Q5_'
            , REGEXP_REPLACE(RESPONSE_VALUE_CODE[OFFSET(offset)], r'^"|"$', '')
        ) AS RESPONSE_QUESTIONID
        , REGEXP_REPLACE(RESPONSE_VALUE_2[OFFSET(offset)], r'^"|"$', '') AS RESPONSE_VALUE
    FROM renamed
    , UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(RESPONSE_VALUE_CODE) - 1, 1)) AS offset

)

, unnested_with_qid12 AS (

    SELECT
        *
        , 'QID12' AS RESPONSE_QUESTIONID
        , REGEXP_REPLACE(RESPONSE_VALUE_QID12, r'^"|"$', '') AS RESPONSE_VALUE
    FROM renamed

)

, unnioned AS (

    SELECT
        RECORD_LOAD_DTS
        , EFFECTIVE_FROM
        , RESPONSE_RESPONSEID
        , RESPONSE_QUESTIONID
        , RESPONSE_VALUE
        , RESPONSE_SURVEYID
        , 'Sentiment' AS RESPONSE_SURVEYTYPE
        , CONCAT(RESPONSE_SURVEYID, RESPONSE_QUESTIONID) AS QUESTION_SURVEYQUESTIONID
        , RESPONSE_STARTDATE
        , RESPONSE_ENDDATE
        , RESPONSE_STATUS
        , RESPONSE_IPADDRESS
        , RESPONSE_DURATION
        , RESPONSE_RECORDEDDATE
        , RESPONSE_LOCATIONLATITUDE
        , RESPONSE_LOCATIONLONGITUDE
        , RESPONSE_REGION_NAME
        , RESPONSE_DISTRIBUTIONCHANNEL
        , RESPONSE_PAGETYPE
        , RESPONSE_SYSENV
        , RESPONSE_PAGEREFERRER
        , RESPONSE_SITEREFERRER
        , RESPONSE_CURRENTPAGEURL
        , RESPONSE_SURVEYID AS QUESTION_SURVEYID
        , '(not set)' AS QUESTION_QUESTIONID
        , '(not set)' AS QUESTION_QUESTIONTEXT
        , '(not set)' AS QUESTION_DEFAULTCHOICES
        , '(not set)' AS QUESTION_DATAEXPORTTAG
        , '(not set)' AS QUESTION_QUESTIONTYPE
        , '(not set)' AS QUESTION_SELECTOR
        , '(not set)' AS QUESTION_DESCRIPTION
        , '(not set)' AS QUESTION_SUBSELECTOR
        , '(not set)' AS QUESTION_CHOICEORDER
        , ARTICLE_KEY
        , RESPONSE_hexa_ACCOUNT_ID
        , CONCAT(
            RESPONSE_SURVEYID, '|'
            , RESPONSE_RESPONSEID, '|'
            , RESPONSE_QUESTIONID
        ) AS RESPONSE_KEY
    FROM unnested
    WHERE RESPONSE_VALUE IS NOT NULL
    UNION ALL
    SELECT
        RECORD_LOAD_DTS
        , EFFECTIVE_FROM
        , RESPONSE_RESPONSEID
        , RESPONSE_QUESTIONID
        , RESPONSE_VALUE
        , RESPONSE_SURVEYID
        , 'Sentiment' AS RESPONSE_SURVEYTYPE
        , CONCAT(RESPONSE_SURVEYID, RESPONSE_QUESTIONID) AS QUESTION_SURVEYQUESTIONID
        , RESPONSE_STARTDATE
        , RESPONSE_ENDDATE
        , RESPONSE_STATUS
        , RESPONSE_IPADDRESS
        , RESPONSE_DURATION
        , RESPONSE_RECORDEDDATE
        , RESPONSE_LOCATIONLATITUDE
        , RESPONSE_LOCATIONLONGITUDE
        , RESPONSE_REGION_NAME
        , RESPONSE_DISTRIBUTIONCHANNEL
        , RESPONSE_PAGETYPE
        , RESPONSE_SYSENV
        , RESPONSE_PAGEREFERRER
        , RESPONSE_SITEREFERRER
        , RESPONSE_CURRENTPAGEURL
        , RESPONSE_SURVEYID AS QUESTION_SURVEYID
        , '(not set)' AS QUESTION_QUESTIONID
        , '(not set)' AS QUESTION_QUESTIONTEXT
        , '(not set)' AS QUESTION_DEFAULTCHOICES
        , '(not set)' AS QUESTION_DATAEXPORTTAG
        , '(not set)' AS QUESTION_QUESTIONTYPE
        , '(not set)' AS QUESTION_SELECTOR
        , '(not set)' AS QUESTION_DESCRIPTION
        , '(not set)' AS QUESTION_SUBSELECTOR
        , '(not set)' AS QUESTION_CHOICEORDER
        , ARTICLE_KEY
        , RESPONSE_hexa_ACCOUNT_ID
        , CONCAT(
            RESPONSE_SURVEYID, '|'
            , RESPONSE_RESPONSEID, '|'
            , RESPONSE_QUESTIONID
        ) AS RESPONSE_KEY
    FROM unnested_with_qid12
    WHERE RESPONSE_VALUE IS NOT NULL

)

SELECT unnioned.* FROM unnioned
