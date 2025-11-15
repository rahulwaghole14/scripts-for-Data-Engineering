WITH raw_data AS (
    SELECT
        tags AS tag
        , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', pub_date) record_load_dts
        , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', pub_date) load_date
        , CAST(REGEXP_EXTRACT(SPLIT(url, '?')[OFFSET(0)], r'(\d{6,})') AS INT64) AS ARTICLE_KEY
    FROM {{ source('parsely', 'parsely_api__tags') }}
    -- filter out urls without article_ids
    WHERE LENGTH(CAST(REGEXP_EXTRACT(SPLIT(url, '?')[OFFSET(0)], r'(\d{6,})$') AS STRING)) >= 6
)

, unnested_data AS (
    SELECT
        ARTICLE_KEY
        , TRIM(REPLACE(tag_value, '"', '')) AS value
        , record_load_dts
        , load_date
    FROM
        raw_data
    , UNNEST(SPLIT(REPLACE(REPLACE(tag, '[', ''), ']', ''), ',')) AS tag_value
)

SELECT
    CAST(ARTICLE_KEY AS INT64) AS ARTICLE_KEY
    , CASE
        WHEN value LIKE '%:iab:%' THEN 'high_level_tags'
        WHEN value LIKE '%:entity:%' THEN 'low_level_tags'
        ELSE 'site_tags'
    END AS TAG_CLASS
    , CASE
        WHEN REGEXP_CONTAINS(value, r":.*:")
            THEN
                LOWER(TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(value, r":.*:(.*)"), r"^'|'$", "")))
        WHEN REGEXP_CONTAINS(value, r":")
            THEN
                LOWER(TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(value, r":(.*)"), r"^'|'$", "")))
        ELSE
            LOWER(TRIM(REGEXP_REPLACE(value, r"^'|'$", "")))
    END AS TAG
    , record_load_dts AS RECORD_LOAD_DTS
    , load_date AS LOAD_DATE
    , CONCAT(
        CASE
            WHEN value LIKE '%:iab:%' THEN 'high_level_tags'
            WHEN value LIKE '%:entity:%' THEN 'low_level_tags'
            ELSE 'site_tags'
        END
        , '|'
        , CASE
            WHEN REGEXP_CONTAINS(value, r":.*:")
                THEN
                    LOWER(TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(value, r":.*:(.*)"), r"^'|'$", "")))
            WHEN REGEXP_CONTAINS(value, r":")
                THEN
                    LOWER(TRIM(REGEXP_REPLACE(REGEXP_EXTRACT(value, r":(.*)"), r"^'|'$", "")))
            ELSE
                LOWER(TRIM(REGEXP_REPLACE(value, r"^'|'$", "")))
        END
    ) AS TAG_KEY
FROM
    unnested_data
