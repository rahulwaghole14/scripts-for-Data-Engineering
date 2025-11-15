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

WITH UniqueVisitors AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS,
        GEO_REGION
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

PageViews AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(*) AS PAGE_VIEWS,
        GEO_REGION
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (POST_PAGENAME != '' OR POST_PAGE_URL != '')
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

Visits AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH, VISIT_NUM)) AS VISIT,
        GEO_REGION
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
)

SELECT
    uv.DAY,
    uv.UNIQUE_VISITORS,
    pv.PAGE_VIEWS,
    v.VISIT,
    uv.GEO_REGION
FROM UniqueVisitors uv
INNER JOIN PageViews pv ON (uv.DAY = pv.DAY AND uv.GEO_REGION = pv.GEO_REGION)
INNER JOIN Visits v ON (uv.DAY = v.DAY AND uv.GEO_REGION = v.GEO_REGION)
