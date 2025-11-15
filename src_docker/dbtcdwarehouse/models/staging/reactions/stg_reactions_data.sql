WITH source AS (
    SELECT *
    FROM {{ source('reactions', 'reactions_data_*') }}
),
renamed AS (
    SELECT
        {{ adapter.quote("timestamp") }} AS TIMESTAMP_UTC,  -- Retain the original timestamp
        DATETIME(TIMESTAMP({{ adapter.quote("timestamp") }}), "Pacific/Auckland") AS TIMESTAMP_NZT,  -- Convert to NZT
        {{ adapter.quote("last_modified_s3") }} AS LAST_MODIFIED_S3_UTC,  -- Retain the original last_modified_s3
        DATETIME(TIMESTAMP({{ adapter.quote("last_modified_s3") }}), "Pacific/Auckland") AS LAST_MODIFIED_NZT,  -- Convert last_modified_s3 to NZT
        {{ adapter.quote("surveyId") }} AS SURVEY_ID,
        ARRAY_TO_STRING({{ adapter.quote("reactions") }}, ', ') AS REACTIONS,  -- Concatenate the reactions array into a single string
        {{ adapter.quote("userId") }} AS USER_ID,
        {{ adapter.quote("accountId") }} AS ACCOUNT_ID,
        {{ adapter.quote("storyId") }} AS STORY_ID,
        {{ adapter.quote("feedback") }} AS FEEDBACK,  -- Keep feedback as a string
        {{ adapter.quote("id") }} AS ID
    FROM source
)
SELECT *
FROM renamed
