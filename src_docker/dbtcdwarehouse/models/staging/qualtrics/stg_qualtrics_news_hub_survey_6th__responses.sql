{{
  config(
    tags=['qualtrics_surveys']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('qualtrics', 'qualtrics__news_hub_survey_6th_responses') }}
)

, regions AS (
     SELECT
        responseId,
        values_locationLatitude,
        values_locationLongitude,
        b.region
    FROM 
        source a
    LEFT JOIN 
        {{ ref('stg_statsnz__region_polygons') }} b
    ON 
        values_locationLatitude IS NOT NULL 
        AND values_locationLatitude <> 'nan'
        AND values_locationLongitude IS NOT NULL 
        AND values_locationLongitude <> 'nan'
        AND ST_CONTAINS(
            b.region_polygon,
            ST_GEOGPOINT(
                CAST(values_locationLongitude AS FLOAT64),
                CAST(values_locationLatitude AS FLOAT64)
            )
        )
    WHERE 
        a.values_locationLatitude IS NOT NULL 
        AND a.values_locationLatitude <> 'nan' 
        AND a.values_locationLongitude IS NOT NULL 
        AND a.values_locationLongitude <> 'nan'

)

, renamed AS (
    SELECT
        source.responseId AS RESPONSE_RESPONSEID
        , survey_id AS RESPONSE_SURVEYID
        , survey_name AS RESPONSE_SURVEYNAME
        , CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', values_recordedDate) AS DATETIME) AS RESPONSE_RECORDEDDATE
        , PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_startDate) AS RESPONSE_STARTDATE
        , PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_endDate) AS RESPONSE_ENDDATE
        , CAST(values_status AS STRING) AS RESPONSE_STATUS
        , values_ipAddress AS RESPONSE_IPADDRESS
        , CAST(values_duration AS INT64) AS RESPONSE_DURATION
        , source.values_locationLatitude AS RESPONSE_LOCATIONLATITUDE
        , source.values_locationLongitude AS RESPONSE_LOCATIONLONGITUDE
        , COALESCE(regions.region, NULL) AS RESPONSE_REGION_NAME
        , values_distributionChannel AS RESPONSE_DISTRIBUTIONCHANNEL
        , values_userLanguage AS RESPONSE_USERLANGUAGE
        , values_Q_RecaptchaScore AS RESPONSE_RECAPTCHASCORE
        , values_QID36_BROWSER AS RESPONSE_BROWSER
        , values_QID36_VERSION AS RESPONSE_BROWSER_VERSION
        , values_QID36_OS AS RESPONSE_OS
        , values_QID36_RESOLUTION AS RESPONSE_RESOLUTION
        , values_QID4 AS RESPONSE_QID4
        , values_QID6 AS RESPONSE_QID6
        , values_QID25 AS RESPONSE_QID25
        , values_QID17 AS RESPONSE_QID17
        , values_QID17_12_TEXT AS RESPONSE_QID17_12_TEXT
        , values_QID18 AS RESPONSE_QID18
        , values_QID19 AS RESPONSE_QID19
        , values_QID9 AS RESPONSE_QID9
        , values_QID10 AS RESPONSE_QID10
        , values_QID11 AS RESPONSE_QID11
        , values_QID12 AS RESPONSE_QID12
        , values_QID33_TEXT AS RESPONSE_QID33_TEXT
        , values_QID20 AS RESPONSE_QID20
        , values_QID20_2_TEXT AS RESPONSE_QID20_2_TEXT
        , values_QID21 AS RESPONSE_QID21
        , values_QID37 AS RESPONSE_QID37
        , values_QID26 AS RESPONSE_QID26
        , values_QID34 AS RESPONSE_QID34
        , values_QID34_7_TEXT AS RESPONSE_QID34_7_TEXT
        , values_QID29_1 AS RESPONSE_QID29_1
        , values_QID29_2 AS RESPONSE_QID29_2
        , values_QID28_1 AS RESPONSE_QID28_1
        , values_QID28_2 AS RESPONSE_QID28_2
        , values_QID28_3 AS RESPONSE_EMAIL
        , labels_status AS RESPONSE_LABELS_STATUS
        , labels_finished AS RESPONSE_LABELS_FINISHED
        , labels_QID4 AS RESPONSE_LABELS_QID4
        , labels_QID6 AS RESPONSE_LABELS_QID6
        , labels_QID25 AS RESPONSE_LABELS_QID25
        , labels_QID17 AS RESPONSE_LABELS_QID17
        , labels_QID18 AS RESPONSE_LABELS_QID18
        , labels_QID19 AS RESPONSE_LABELS_QID19
        , labels_QID9 AS RESPONSE_LABELS_QID9
        , labels_QID10 AS RESPONSE_LABELS_QID10
        , labels_QID11 AS RESPONSE_LABELS_QID11
        , labels_QID12 AS RESPONSE_LABELS_QID12
        , labels_QID20 AS RESPONSE_LABELS_QID20
        , labels_QID21 AS RESPONSE_LABELS_QID21
        , labels_QID37 AS RESPONSE_LABELS_QID37
        , labels_QID26 AS RESPONSE_LABELS_QID26
        , labels_QID34 AS RESPONSE_LABELS_QID34
        , labels_QID29_1 AS RESPONSE_LABELS_QID29_1
        , labels_QID29_2 AS RESPONSE_LABELS_QID29_2
        , displayedValues_QID19 AS RESPONSE_DISPLAYEDVALUES_QID19
        , displayedValues_QID18 AS RESPONSE_DISPLAYEDVALUES_QID18
        , displayedValues_QID17 AS RESPONSE_DISPLAYEDVALUES_QID17
        , displayedValues_QID37 AS RESPONSE_DISPLAYEDVALUES_QID37
        , displayedValues_QID34 AS RESPONSE_DISPLAYEDVALUES_QID34
        , displayedValues_QID12 AS RESPONSE_DISPLAYEDVALUES_QID12
        , displayedValues_QID11 AS RESPONSE_DISPLAYEDVALUES_QID11
        , displayedValues_QID10 AS RESPONSE_DISPLAYEDVALUES_QID10
        , displayedValues_QID29_1 AS RESPONSE_DISPLAYEDVALUES_QID29_1
        , displayedValues_QID29_2 AS RESPONSE_DISPLAYEDVALUES_QID29_2
        , displayedValues_QID26 AS RESPONSE_DISPLAYEDVALUES_QID26
        , displayedValues_QID9 AS RESPONSE_DISPLAYEDVALUES_QID9
        , displayedValues_QID4 AS RESPONSE_DISPLAYEDVALUES_QID4
        , displayedValues_QID6 AS RESPONSE_DISPLAYEDVALUES_QID6
        , displayedValues_QID25 AS RESPONSE_DISPLAYEDVALUES_QID25
        , displayedValues_QID21 AS RESPONSE_DISPLAYEDVALUES_QID21
        , displayedValues_QID20 AS RESPONSE_DISPLAYEDVALUES_QID20
        , values_QID35_TEXT AS RESPONSE_QID35_TEXT
        , values_QID9_9_TEXT AS RESPONSE_QID9_9_TEXT
        , values_QID37_12_TEXT AS RESPONSE_QID37_12_TEXT
        , values_QID13 AS RESPONSE_QID13
        , values_QID14_1 AS RESPONSE_QID14_1
        , values_QID14_8 AS RESPONSE_QID14_8
        , values_QID14_9 AS RESPONSE_QID14_9
        , values_QID14_10 AS RESPONSE_QID14_10
        , values_QID14_11 AS RESPONSE_QID14_11
        , values_QID14_12 AS RESPONSE_QID14_12
        , values_QID14_13 AS RESPONSE_QID14_13
        , values_QID14_14 AS RESPONSE_QID14_14
        , values_QID15 AS RESPONSE_QID15
        , labels_QID13 AS RESPONSE_LABELS_QID13
        , labels_QID14_1 AS RESPONSE_LABELS_QID14_1
        , labels_QID14_8 AS RESPONSE_LABELS_QID14_8
        , labels_QID14_9 AS RESPONSE_LABELS_QID14_9
        , labels_QID14_10 AS RESPONSE_LABELS_QID14_10
        , labels_QID14_11 AS RESPONSE_LABELS_QID14_11
        , labels_QID14_12 AS RESPONSE_LABELS_QID14_12
        , labels_QID14_13 AS RESPONSE_LABELS_QID14_13
        , labels_QID14_14 AS RESPONSE_LABELS_QID14_14
        , labels_QID15 AS RESPONSE_LABELS_QID15
        , displayedValues_QID15 AS RESPONSE_DISPLAYEDVALUES_QID15
        , displayedValues_QID13 AS RESPONSE_DISPLAYEDVALUES_QID13
        , displayedValues_QID14_14 AS RESPONSE_DISPLAYEDVALUES_QID14_14
        , displayedValues_QID14_13 AS RESPONSE_DISPLAYEDVALUES_QID14_13
        , displayedValues_QID14_12 AS RESPONSE_DISPLAYEDVALUES_QID14_12
        , displayedValues_QID14_11 AS RESPONSE_DISPLAYEDVALUES_QID14_11
        , displayedValues_QID14_10 AS RESPONSE_DISPLAYEDVALUES_QID14_10
        , displayedValues_QID14_9 AS RESPONSE_DISPLAYEDVALUES_QID14_9
        , displayedValues_QID14_8 AS RESPONSE_DISPLAYEDVALUES_QID14_8
        , displayedValues_QID14_1 AS RESPONSE_DISPLAYEDVALUES_QID14_1
        , values_QID18_x12_TEXT AS RESPONSE_QID18_x12_TEXT
        , values_QID21_12_TEXT AS RESPONSE_QID21_12_TEXT
        , values_QID15_8_TEXT AS RESPONSE_QID15_8_TEXT
        , values_QID13_9_TEXT AS RESPONSE_QID13_9_TEXT
        , values_QID6_5_TEXT AS RESPONSE_QID6_5_TEXT
    FROM source
    LEFT JOIN 
        regions 
    ON 
        source.responseId = regions.responseId
)

SELECT * FROM renamed
