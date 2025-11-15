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


WITH UniqueVisitors AS (
    SELECT
        DATE(LOAD_DATE)
            AS DAY -- Assuming LOAD_DATE is in the correct timezone
        , POST_EVAR6 AS ARTICLE_ID
        , GEO_REGION
        , GEO_CITY
        , COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS UNIQUE_VISITORS
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE LOWER(GEO_COUNTRY) = 'nzl' AND POST_EVAR6 IS NOT null
        {% if is_incremental() %}
            {#  evaluates to true for incremental models #}
            AND DATE(LOAD_DATE) >= DATE_SUB(current_date(), INTERVAL 1 DAY)
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}
    GROUP BY 1, 2, 3, 4
)

, PageViews AS (
    SELECT
        DATE(LOAD_DATE)
            AS DAY -- Assuming LOAD_DATE is in the correct timezone
        , POST_EVAR6 AS ARTICLE_ID
        , GEO_REGION
        , GEO_CITY
        , COUNT(*) AS PAGE_VIEWS
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE LOWER(GEO_COUNTRY) = 'nzl' AND POST_EVAR6 IS NOT null
        {% if is_incremental() %}
            {#  evaluates to true for incremental models #}
            AND DATE(LOAD_DATE) >= DATE_SUB(current_date(), INTERVAL 1 DAY)
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}
    GROUP BY 1, 2, 3, 4
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
        , GEO_CITY
        , POST_EVAR6 AS ARTICLE_ID
        , SAFE_CAST(VISIT_PAGE_NUM AS int64) AS VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT AS int64) AS POST_CUST_HIT_TIME_GMT
        , SAFE_CAST(VISIT_START_TIME_GMT AS int64) AS VISIT_START_TIME_GMT
        , DATE(LOAD_DATE) AS DAY
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE LOWER(GEO_COUNTRY) = 'nzl' AND POST_EVAR6 IS NOT null
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
        , GEO_REGION
        , GEO_CITY
        , ARTICLE_ID
        , LAST_VALUE(POST_CUST_HIT_TIME_GMT) OVER (
            PARTITION BY VISIT_HASH
            ORDER BY VISIT_PAGE_NUM
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_hit_time
    FROM pages_replatform
)

, grouped AS (
    SELECT VISIT_HASH, first_visit_length.DAY, ANY_VALUE(GEO_REGION) AS GEO_REGION, ANY_VALUE(GEO_CITY) AS GEO_CITY, ANY_VALUE(ARTICLE_ID) AS ARTICLE_ID, ANY_VALUE(VISIT_START_TIME_GMT) AS VISIT_START_TIME_GMT, ANY_VALUE(last_hit_time) AS last_hit_time FROM first_visit_length GROUP BY 1, 2
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
        , ARTICLE_ID
        , GEO_REGION
        , GEO_CITY,
    FROM grouped
    GROUP BY 2, 3, 4, 5

)

, content_article_metrics_region AS (
    SELECT
        uv.DAY
        , uv.UNIQUE_VISITORS
        , uv.ARTICLE_ID
        , uv.GEO_REGION
        , uv.GEO_CITY
        , pv.PAGE_VIEWS
        , tts.TOTAL_TIME_SPENT_SECONDS
        , ROUND(CAST(tts.TOTAL_TIME_SPENT_SECONDS AS FLOAT64) / CAST(uv.UNIQUE_VISITORS AS FLOAT64), 1) AS TIME_SPENT_PER_VISITOR
        , ROUND(CAST(tts.TOTAL_TIME_SPENT_SECONDS AS FLOAT64) / CAST(pv.PAGE_VIEWS AS FLOAT64), 1) AS TIME_SPENT_PER_VISIT
    FROM UniqueVisitors uv
    LEFT JOIN PageViews pv ON (uv.DAY = pv.DAY AND uv.ARTICLE_ID = pv.ARTICLE_ID AND uv.GEO_REGION = pv.GEO_REGION AND uv.GEO_CITY = pv.GEO_CITY)
    LEFT JOIN total_time_spent tts ON (uv.DAY = tts.DAY AND uv.ARTICLE_ID = tts.ARTICLE_ID AND uv.GEO_REGION = pv.GEO_REGION AND uv.GEO_CITY = pv.GEO_CITY)

    ORDER BY 2 DESC
)

, sponsored_content_drupal AS (
    SELECT DISTINCT ARTICLE_ID, ARTICLE_HEADLINE, ARTICLE_CREATEDDATE
    FROM {{ ref('int_drupal__all_sponsored_articles') }}
)

, final_sponsored_content AS (
    SELECT
        cm.DAY AS DATE_AT
        , cm.DAY
        , cm.GEO_REGION
        , cm.GEO_CITY
        , cm.ARTICLE_ID
        , sc.ARTICLE_HEADLINE
        , cm.UNIQUE_VISITORS
        , cm.PAGE_VIEWS
        , cm.TOTAL_TIME_SPENT_SECONDS
        , cm.TIME_SPENT_PER_VISITOR
        , cm.TIME_SPENT_PER_VISIT
        , sc.ARTICLE_CREATEDDATE AS ARTICLE_CREATED_AT
    FROM content_article_metrics_region cm
    JOIN sponsored_content_drupal sc
        ON cm.ARTICLE_ID = sc.ARTICLE_ID
)

SELECT * FROM final_sponsored_content
