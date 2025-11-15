{{
  config(
    tags=['qualtrics_hexa_reader']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('qualtrics', 'qualtrics__reader_feedback_survey_responses') }}
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
        source.responseId AS RESPONSE_RESPONSEID,
        survey_id AS RESPONSE_SURVEYID,
        survey_name AS RESPONSE_SURVEYNAME,
        displayedFields AS RESPONSE_DISPLAYEDFIELDS,
        PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_startDate) AS RESPONSE_STARTDATE,
        PARSE_DATETIME('%Y-%m-%dT%H:%M:%SZ', values_endDate) AS RESPONSE_ENDDATE,
        CAST(values_status AS STRING) AS RESPONSE_STATUS,
        values_ipAddress AS RESPONSE_IPADDRESS,
        values_progress AS RESPONSE_PROGRESS,
        CAST(values_duration AS INT64) AS RESPONSE_DURATION,
        CAST(values_finished AS STRING) AS RESPONSE_FINISHED,
        CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', values_recordedDate) AS DATETIME) AS RESPONSE_RECORDEDDATE,
        values__recordId AS RESPONSE_RECORDID,
        source.values_locationLatitude AS RESPONSE_LOCATIONLATITUDE,
        source.values_locationLongitude AS RESPONSE_LOCATIONLONGITUDE,
        COALESCE(regions.region, NULL) AS RESPONSE_REGION_NAME,
        values_distributionChannel AS RESPONSE_DISTRIBUTIONCHANNEL,
        values_userLanguage AS RESPONSE_USERLANGUAGE,
        values_Q_RecaptchaScore AS RESPONSE_RECAPTCHASCORE,
        values_QID38_BROWSER AS RESPONSE_BROWSER,
        values_QID38_VERSION AS RESPONSE_BROWSER_VERSION,
        values_QID38_OS AS RESPONSE_OS,
        values_QID38_RESOLUTION AS RESPONSE_RESOLUTION,
        values_QID4 AS RESPONSE_QID4,
        labels_status AS RESPONSE_LABELS_STATUS,
        labels_finished AS RESPONSE_LABELS_FINISHED,
        labels_QID4 AS RESPONSE_AGE_BUCKET,
        displayedValues_QID4 AS RESPONSE_DISPLAYEDVALUES_QID4,
        values_QID6 AS RESPONSE_QID6,
        values_QID25 AS RESPONSE_QID25,
        values_QID17 AS RESPONSE_QID17,
        values_QID18 AS RESPONSE_QID18,
        values_QID19 AS RESPONSE_QID19,
        values_QID10 AS RESPONSE_QID10,
        values_QID12 AS RESPONSE_QID12,
        values_QID33_TEXT AS RESPONSE_QID33_TEXT,
        values_QID26 AS RESPONSE_QID26,
        values_QID34 AS RESPONSE_QID34,
        values_QID35_TEXT AS RESPONSE_QID35_TEXT,
        values_QID29_1 AS RESPONSE_QID29_1,
        values_QID29_2 AS RESPONSE_QID29_2,
        labels_QID6 AS RESPONSE_GENDER,
        labels_QID25 AS RESPONSE_LOCATION,
        labels_QID17 AS RESPONSE_LABELS_QID17,
        labels_QID18 AS RESPONSE_LABELS_QID18,
        labels_QID19 AS RESPONSE_LABELS_QID19,
        labels_QID10 AS RESPONSE_LABELS_QID10,
        labels_QID12 AS RESPONSE_LABELS_QID12,
        labels_QID26 AS RESPONSE_LABELS_QID26,
        labels_QID34 AS RESPONSE_LABELS_QID34,
        labels_QID29_1 AS RESPONSE_LABELS_QID29_1,
        labels_QID29_2 AS RESPONSE_LABELS_QID29_2,
        displayedValues_QID19 AS RESPONSE_DISPLAYEDVALUES_QID19,
        displayedValues_QID18 AS RESPONSE_DISPLAYEDVALUES_QID18,
        displayedValues_QID29_1 AS RESPONSE_DISPLAYEDVALUES_QID29_1,
        displayedValues_QID17 AS RESPONSE_DISPLAYEDVALUES_QID17,
        displayedValues_QID29_2 AS RESPONSE_DISPLAYEDVALUES_QID29_2,
        displayedValues_QID26 AS RESPONSE_DISPLAYEDVALUES_QID26,
        displayedValues_QID6 AS RESPONSE_DISPLAYEDVALUES_QID6,
        displayedValues_QID25 AS RESPONSE_DISPLAYEDVALUES_QID25,
        displayedValues_QID34 AS RESPONSE_DISPLAYEDVALUES_QID34,
        displayedValues_QID12 AS RESPONSE_DISPLAYEDVALUES_QID12,
        displayedValues_QID10 AS RESPONSE_DISPLAYEDVALUES_QID10,
        values_QID13 AS RESPONSE_QID13,
        values_QID14_1 AS RESPONSE_QID14_1,
        values_QID14_8 AS RESPONSE_QID14_8,
        values_QID14_9 AS RESPONSE_QID14_9,
        values_QID14_10 AS RESPONSE_QID14_10,
        values_QID14_11 AS RESPONSE_QID14_11,
        values_QID14_12 AS RESPONSE_QID14_12,
        values_QID14_13 AS RESPONSE_QID14_13,
        values_QID15 AS RESPONSE_QID15,
        values_QID20 AS RESPONSE_QID20,
        values_QID21 AS RESPONSE_QID21,
        values_QID21_12_TEXT AS RESPONSE_QID21_12_TEXT,
        values_QID34_7_TEXT AS RESPONSE_QID34_7_TEXT,
        labels_QID13 AS RESPONSE_LABELS_QID13,
        labels_QID14_1 AS RESPONSE_LABELS_QID14_1,
        labels_QID14_8 AS RESPONSE_LABELS_QID14_8,
        labels_QID14_9 AS RESPONSE_LABELS_QID14_9,
        labels_QID14_10 AS RESPONSE_LABELS_QID14_10,
        labels_QID14_11 AS RESPONSE_LABELS_QID14_11,
        labels_QID14_12 AS RESPONSE_LABELS_QID14_12,
        labels_QID14_13 AS RESPONSE_LABELS_QID14_13,
        labels_QID15 AS RESPONSE_LABELS_QID15,
        labels_QID20 AS RESPONSE_LABELS_QID20,
        labels_QID21 AS RESPONSE_LABELS_QID21,
        displayedValues_QID15 AS RESPONSE_DISPLAYEDVALUES_QID15,
        displayedValues_QID13 AS RESPONSE_DISPLAYEDVALUES_QID13,
        displayedValues_QID14_13 AS RESPONSE_DISPLAYEDVALUES_QID14_13,
        displayedValues_QID14_12 AS RESPONSE_DISPLAYEDVALUES_QID14_12,
        displayedValues_QID14_11 AS RESPONSE_DISPLAYEDVALUES_QID14_11,
        displayedValues_QID14_10 AS RESPONSE_DISPLAYEDVALUES_QID14_10,
        displayedValues_QID14_9 AS RESPONSE_DISPLAYEDVALUES_QID14_9,
        displayedValues_QID14_8 AS RESPONSE_DISPLAYEDVALUES_QID14_8,
        displayedValues_QID21 AS RESPONSE_DISPLAYEDVALUES_QID21,
        displayedValues_QID20 AS RESPONSE_DISPLAYEDVALUES_QID20,
        displayedValues_QID14_1 AS RESPONSE_DISPLAYEDVALUES_QID14_1,
        values_QID28_1 AS RESPONSE_NAME,
        values_QID28_2 AS RESPONSE_PHONE_NUMBER,
        values_QID28_3 AS RESPONSE_EMAIL,
        values_QID17_12_TEXT AS RESPONSE_QID17_12_TEXT,
        values_QID18_x12_TEXT AS RESPONSE_QID18_x12_TEXT,
        values_QID20_2_TEXT AS RESPONSE_QID20_2_TEXT,
        values_QID15_8_TEXT AS RESPONSE_QID15_8_TEXT,
        values_QID6_5_TEXT AS RESPONSE_QID6_5_TEXT
    FROM source
    LEFT JOIN 
        regions 
    ON 
        source.responseId = regions.responseId
)

SELECT * FROM renamed
