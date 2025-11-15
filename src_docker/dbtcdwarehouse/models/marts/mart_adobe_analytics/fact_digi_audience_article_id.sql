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
        POST_EVAR6 AS ARTICLE_ID
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Use this as the start date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

PageViews AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(*) AS PAGE_VIEWS,
        POST_EVAR6 AS ARTICLE_ID
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (POST_PAGENAME != '' OR POST_PAGE_URL != '')
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Use this as the start date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
),

Visit AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH, VISIT_NUM)) AS VISIT,
        POST_EVAR6 AS ARTICLE_ID
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND (
            {% if is_incremental() %}
            DATE(LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
            {% else %}
            DATE(LOAD_DATE) >= '2024-01-01'  -- Use this as the start date for full runs
            {% endif %}
        )
    GROUP BY 1, 3
)

-- Aggregate PAGE_VIEWS, UNIQUE_VISITORS, and VISIT
, aggregated_data AS (
    SELECT
        uv.DAY,
        uv.ARTICLE_ID,
        SUM(uv.UNIQUE_VISITORS) AS UNIQUE_VISITORS,
        SUM(pv.PAGE_VIEWS) AS PAGE_VIEWS,
        SUM(v.VISIT) AS VISIT
    FROM UniqueVisitors uv
    INNER JOIN PageViews pv ON (uv.DAY = pv.DAY AND uv.ARTICLE_ID = pv.ARTICLE_ID)
    INNER JOIN Visit v ON (uv.DAY = v.DAY AND uv.ARTICLE_ID = v.ARTICLE_ID)
    GROUP BY uv.DAY, uv.ARTICLE_ID
)

-- Mark "Other" for low PAGE_VIEWS articles
, final_data AS (
    SELECT
        DAY,
        ARTICLE_ID,
        SUM(UNIQUE_VISITORS) AS UNIQUE_VISITORS,
        SUM(PAGE_VIEWS) AS PAGE_VIEWS,
        SUM(VISIT) AS VISIT,
        CASE
            WHEN SUM(PAGE_VIEWS) < 500 THEN 'Other'
            ELSE ARTICLE_ID
        END AS ADJUSTED_ARTICLE_ID
    FROM aggregated_data
    GROUP BY DAY, ARTICLE_ID
)

-- Final result with the count of "Other" articles and other values
, other_articles AS (
    SELECT
        DAY,
        'Other' AS ARTICLE_ID,
        SUM(UNIQUE_VISITORS) AS UNIQUE_VISITORS,
        SUM(PAGE_VIEWS) AS PAGE_VIEWS,
        SUM(VISIT) AS VISIT,
        COUNT(DISTINCT ARTICLE_ID) AS NUMBER_OF_ARTICLES
    FROM final_data
    WHERE ADJUSTED_ARTICLE_ID = 'Other'
    GROUP BY DAY
)

, non_other_articles AS (
    SELECT
        DAY,
        ARTICLE_ID,
        SUM(UNIQUE_VISITORS) AS UNIQUE_VISITORS,
        SUM(PAGE_VIEWS) AS PAGE_VIEWS,
        SUM(VISIT) AS VISIT,
        1 AS NUMBER_OF_ARTICLES
    FROM final_data
    WHERE ADJUSTED_ARTICLE_ID != 'Other'
    GROUP BY DAY,ARTICLE_ID
)

-- Combine "Other" and non-"Other" articles
SELECT
    DAY,
    ARTICLE_ID,
    PAGE_VIEWS,
    VISIT,
    UNIQUE_VISITORS,
    NUMBER_OF_ARTICLES
FROM non_other_articles

UNION ALL

SELECT
    DAY,
    ARTICLE_ID,
    PAGE_VIEWS,
    VISIT,
    UNIQUE_VISITORS,
    NUMBER_OF_ARTICLES
FROM other_articles
