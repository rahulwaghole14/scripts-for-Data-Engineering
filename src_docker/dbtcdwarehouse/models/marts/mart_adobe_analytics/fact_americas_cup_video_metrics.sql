{{
  config(
    tags=['adobe'],
    materialized='incremental',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "DAY",
      "data_type": "date",
      "granularity": "DAY"
    }
  )
}}

WITH pv_uv AS (
    SELECT
        DATE(LOAD_DATE) AS DAY,  -- Assuming LOAD_DATE is in the correct timezone
        POST_EVAR6 AS ARTICLE_ID,
        POST_PAGE_URL,
        COUNT(*) AS PAGE_VIEWS,
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS,
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND LOWER(POST_PAGE_URL) LIKE '%americas%'
        {% if is_incremental() %}
            AND DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        {% else %}
            AND DATE(LOAD_DATE) >= '2024-08-20'
        {% endif %}
    GROUP BY 2, 1, 3
),

bc AS (
    SELECT
        *,  -- This selects all columns from the table
        REGEXP_EXTRACT(destination_path, r'/(\d+)/') AS ARTICLE_ID_BC
    FROM `hexa-data-report-etl-prod.cdw_stage.brightcove__daily_videos_destination`
    WHERE destination_path LIKE '%americas%'
)

SELECT
    pv_uv.DAY,
    pv_uv.ARTICLE_ID,
    pv_uv.POST_PAGE_URL,
    bc.video_seconds_viewed,
    bc.video_view,
    bc.video_impression,
    bc.video_percent_viewed,
    SUM(pv_uv.PAGE_VIEWS) AS PAGE_VIEWS,  -- Summing PAGE_VIEWS
    SUM(pv_uv.UNIQUE_VISITORS) AS UNIQUE_VISITORS,  -- Summing UNIQUE_VISITORS
FROM pv_uv
INNER JOIN bc ON (pv_uv.DAY = bc.date AND pv_uv.ARTICLE_ID = bc.ARTICLE_ID_BC)
GROUP BY 1,2,3,4,5,6,7
