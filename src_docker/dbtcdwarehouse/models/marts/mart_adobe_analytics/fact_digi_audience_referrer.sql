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
        REF_TYPE
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) = '2024-08-18'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

PageViews AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(*) AS PAGE_VIEWS,
        REF_TYPE
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (POST_PAGENAME != '' OR POST_PAGE_URL != '')
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) = '2024-08-18'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

Visits AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH, VISIT_NUM)) AS VISIT,
        REF_TYPE
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) = '2024-08-18'  -- Fixed date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
)

, tab AS (
    SELECT
        uv.DAY,
        uv.UNIQUE_VISITORS,
        pv.PAGE_VIEWS,
        v.VISIT,
        uv.REF_TYPE
    FROM UniqueVisitors uv
    INNER JOIN PageViews pv ON (uv.DAY = pv.DAY AND uv.REF_TYPE = pv.REF_TYPE)
    INNER JOIN Visits v ON (uv.DAY = v.DAY AND uv.REF_TYPE = v.REF_TYPE)
)

, reftype AS (
    SELECT * FROM {{ ref('adobe_analytics_replatform_referrer_type') }}
)

SELECT
    tab.DAY,
    tab.UNIQUE_VISITORS,
    tab.PAGE_VIEWS,
    tab.VISIT,
    reftype.NAME AS REFERRER_TYPE
FROM tab
INNER JOIN reftype
    ON tab.REF_TYPE = reftype.ID
