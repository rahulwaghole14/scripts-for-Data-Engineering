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



WITH profileid AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS,
        POST_EVAR7 AS PROFILE_ID
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND POST_EVAR7 IS NOT NULL
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Start date for non-incremental/full runs
            {% endif %}
        )
    GROUP BY 1, 3
)

SELECT
    DAY,
    SUM(UNIQUE_VISITORS) AS UNIQUE_VISITORS,
    PROFILE_ID
FROM profileid
GROUP BY 1,3
