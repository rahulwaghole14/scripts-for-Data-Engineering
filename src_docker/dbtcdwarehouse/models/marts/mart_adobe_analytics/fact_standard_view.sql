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
        DATE(LOAD_DATE)
            AS DAY -- Assuming LOAD_DATE is in the correct timezone
        , GEO_REGION
        , COUNT(*) AS PAGE_VIEWS
        , COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS
        , COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH, VISIT_NUM)) AS VISIT
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl' AND POST_EVAR6 IS NOT null

        {% if is_incremental() %}
            {#  evaluates to true for incremental models #}
            AND DATE(LOAD_DATE) >= DATE_SUB(current_date(), INTERVAL 1 DAY)
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}    GROUP BY 1, 2
)

, pages_replatform AS (

    SELECT
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||'
            , IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
            , IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||'
            , IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) AS VISIT_HASH
        , GEO_REGION
        , SAFE_CAST(VISIT_PAGE_NUM AS int64) AS VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT AS int64) AS POST_CUST_HIT_TIME_GMT
        , SAFE_CAST(VISIT_START_TIME_GMT AS int64) AS VISIT_START_TIME_GMT
        , DATE(LOAD_DATE) AS DAY
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        LOWER(GEO_COUNTRY) = 'nzl' AND POST_EVAR6 IS NOT null
        {% if is_incremental() %}
            {#  evaluates to true for incremental models #}
            AND DATE(LOAD_DATE) >= DATE_SUB(current_date(), INTERVAL 1 DAY)
        {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)


, first_visit_length AS (
    SELECT
        VISIT_HASH
        , VISIT_START_TIME_GMT
        , DAY
        , LAST_VALUE(POST_CUST_HIT_TIME_GMT) OVER (
            PARTITION BY VISIT_HASH
            ORDER BY VISIT_PAGE_NUM
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_hit_time
    FROM pages_replatform
)

, grouped AS (
    SELECT VISIT_HASH, first_visit_length.DAY, ANY_VALUE(VISIT_START_TIME_GMT) AS VISIT_START_TIME_GMT, ANY_VALUE(last_hit_time) AS last_hit_time FROM first_visit_length GROUP BY 1, 2
)

, total_time_spent AS (

    SELECT
        SUM(ABS(
            TIMESTAMP_DIFF(
                TIMESTAMP_SECONDS(last_hit_time)
                , TIMESTAMP_SECONDS(VISIT_START_TIME_GMT)
                , SECOND
            )
        )) AS TOTAL_TIME_SPENT_SECONDS
        , DAY
    FROM grouped
    GROUP BY 2
)




SELECT
    pv_uv.DAY
    , pv_uv.PAGE_VIEWS
    , pv_uv.UNIQUE_VISITORS
    , pv_uv.GEO_REGION
    , pv_uv.VISIT
    , ROUND(CAST(tts.TOTAL_TIME_SPENT_SECONDS AS FLOAT64) / CAST(pv_uv.PAGE_VIEWS AS FLOAT64), 1) AS TIME_SPENT_PER_VISITOR
    , ROUND(CAST(tts.TOTAL_TIME_SPENT_SECONDS AS FLOAT64) / CAST(pv_uv.UNIQUE_VISITORS AS FLOAT64), 1) AS TIME_SPENT_PER_VISIT,
FROM pv_uv
LEFT JOIN total_time_spent tts ON (pv_uv.DAY = tts.DAY)
