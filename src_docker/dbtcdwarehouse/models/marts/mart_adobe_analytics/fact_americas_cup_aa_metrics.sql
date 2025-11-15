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
        POST_EVAR8 AS USER_STATUS,
        POST_PAGE_URL,
        COUNT(*) AS PAGE_VIEWS,
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS,
        COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH, VISIT_NUM)) AS VISIT
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND LOWER(POST_PAGE_URL) LIKE '%americas%'
        {% if is_incremental() %}
            AND DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        {% else %}
            AND DATE(LOAD_DATE) >= '2024-08-20'
        {% endif %}
    GROUP BY 2, 1, 3, 4
),

pages_replatform AS (
    SELECT
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^')
        )))) AS STRING) AS VISIT_HASH,
        POST_EVAR6 AS ARTICLE_ID,
        POST_EVAR8 AS USER_STATUS,
        SAFE_CAST(VISIT_PAGE_NUM AS int64) AS VISIT_PAGE_NUM,
        SAFE_CAST(POST_CUST_HIT_TIME_GMT AS int64) AS POST_CUST_HIT_TIME_GMT,
        SAFE_CAST(VISIT_START_TIME_GMT AS int64) AS VISIT_START_TIME_GMT,
        DATE(LOAD_DATE) AS DAY
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl'
        AND LOWER(POST_PAGE_URL) LIKE '%americas%'
        {% if is_incremental() %}
            AND DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        {% else %}
            AND DATE(LOAD_DATE) >= '2024-08-20'
        {% endif %}
),

first_visit_length AS (
    SELECT
        VISIT_HASH,
        VISIT_START_TIME_GMT,
        DAY,
        ARTICLE_ID,
        USER_STATUS,
        LAST_VALUE(POST_CUST_HIT_TIME_GMT) OVER (
            PARTITION BY VISIT_HASH
            ORDER BY VISIT_PAGE_NUM
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_hit_time
    FROM pages_replatform
),

grouped AS (
    SELECT
        VISIT_HASH,
        first_visit_length.DAY,
        first_visit_length.ARTICLE_ID,
        first_visit_length.USER_STATUS,
        ANY_VALUE(VISIT_START_TIME_GMT) AS VISIT_START_TIME_GMT,
        ANY_VALUE(last_hit_time) AS last_hit_time
    FROM first_visit_length
    GROUP BY 1, 2, 3, 4
),

total_time_spent AS (
    SELECT
        SUM(ABS(
            TIMESTAMP_DIFF(
                TIMESTAMP_SECONDS(last_hit_time),
                TIMESTAMP_SECONDS(VISIT_START_TIME_GMT),
                SECOND
            )
        )) AS TOTAL_TIME_SPENT_SECONDS,
        DAY,
        ARTICLE_ID,
        USER_STATUS
    FROM grouped
    GROUP BY 2, 3, 4
)


SELECT
    pv_uv.DAY,
    SUM(pv_uv.PAGE_VIEWS) AS PAGE_VIEWS,  -- Summing PAGE_VIEWS
    SUM(pv_uv.UNIQUE_VISITORS) AS UNIQUE_VISITORS,  -- Summing UNIQUE_VISITORS
    pv_uv.USER_STATUS,
    pv_uv.ARTICLE_ID,
    pv_uv.POST_PAGE_URL,
    ROUND(CAST(SUM(tts.TOTAL_TIME_SPENT_SECONDS) AS FLOAT64) / CAST(SUM(pv_uv.PAGE_VIEWS) AS FLOAT64), 1) AS TIME_SPENT_PER_VISITOR,
    ROUND(CAST(SUM(tts.TOTAL_TIME_SPENT_SECONDS) AS FLOAT64) / CAST(SUM(pv_uv.UNIQUE_VISITORS) AS FLOAT64), 1) AS TIME_SPENT_PER_VISIT
FROM pv_uv
INNER JOIN total_time_spent tts ON (pv_uv.DAY = tts.DAY AND pv_uv.ARTICLE_ID = tts.ARTICLE_ID AND pv_uv.USER_STATUS = tts.USER_STATUS)
GROUP BY 1, 4, 5, 6
