{{
    config(
        tags = ['drupal_content']
    )
}}

WITH source AS (

    SELECT
        id AS ARTICLE_ID
        , topics AS ARTICLE_TOPICS
        , entities AS ARTICLE_ENTITIES
        , SAFE_CAST(COALESCE(updatedDate, publishedDate, createdDate) AS TIMESTAMP) AS RECORD_LOAD_DTS
    FROM {{ source('drupal_dpa', 'drupal__articles') }}

)

, unnested AS (

    SELECT
        SAFE_CAST(ARTICLE_ID AS INT64) AS ARTICLE_KEY
        , CONCAT(TRIM(LOWER(topic)), '|drupal_topic') AS TAG_KEY
        , TRIM(LOWER(topic)) AS TAG
        , 'drupal_topic' AS TAG_CLASS
        , RECORD_LOAD_DTS
    FROM source
    , UNNEST(ARTICLE_TOPICS) AS topic

    UNION ALL

    SELECT
        SAFE_CAST(ARTICLE_ID AS INT64) AS ARTICLE_KEY
        , CONCAT(TRIM(LOWER(entity)), '|drupal_entity') AS TAG_KEY
        , TRIM(LOWER(entity)) AS TAG
        , 'drupal_entity' AS TAG_CLASS
        , RECORD_LOAD_DTS
    FROM source
    , UNNEST(ARTICLE_ENTITIES) AS entity

)

SELECT * FROM unnested
