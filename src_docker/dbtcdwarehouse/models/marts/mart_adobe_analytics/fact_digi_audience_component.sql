{% set partitions_to_replace = [
  'DATE(CURRENT_DATE("Pacific/Auckland"))',
  'DATE(DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 DAY))',
  'DATE(DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 2 DAY))'
] -%}

{{-
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='sync_all_columns',
    tags = ['adobe'],
    partition_by = {"field": "DAY", "data_type": "date", "granularity": "day"},
    partitions = partitions_to_replace
  )
-}}


WITH component AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(*) AS ARTICLE_CLICK,
        POST_EVAR15 AS COMPONENT_LOCATION
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        DATE(LOAD_DATE) = '2024-08-18'
        AND LOWER(GEO_COUNTRY) = 'nzl'
        AND LOWER(POST_EVAR14) = 'article-click'
        AND POST_EVAR15 is not NULL
        AND (
            REGEXP_CONTAINS(EVENT_LIST, r'\b208\b')
            OR REGEXP_CONTAINS(EVENT_LIST, r'\b209=1.77\b')
            OR REGEXP_CONTAINS(EVENT_LIST, r'\b102\b')
            -- Continue for other event numbers like 103, 104, etc.
            OR REGEXP_CONTAINS(EVENT_LIST, r'\b138\b')
        )
    GROUP BY 1,3
)

SELECT * FROM component
