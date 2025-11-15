{{ config(
    materialized='table',
    unique_key='response_responseid',
    tags=['qualtrics_surveys']
) }}

WITH combined_data AS (
    SELECT
        'newshub2024' AS survey_name,
        LOWER(TRIM(COALESCE(response_email, NULL))) AS response_email
    FROM {{ ref('stg_qualtrics_news_hub_survey__responses') }}
    UNION ALL
    SELECT
        'hexa_reader_feedback_survey_2024' AS survey_name,
        LOWER(TRIM(COALESCE(response_email, NULL))) AS response_email
    FROM {{ ref('stg_qualtrics_news_hub_survey__responses') }}
    UNION ALL
    SELECT
        'newshub2024_July_6th' AS survey_name,
        LOWER(TRIM(COALESCE(response_email, NULL))) AS response_email
    FROM {{ ref('stg_qualtrics_news_hub_survey_6th__responses') }}

)

SELECT
    {{ format_uuid(generate_uuid_v5("response_email")) }} AS marketing_id,
    combined_data.*
FROM combined_data
WHERE response_email is not NULL 
      AND response_email <> 'nan'