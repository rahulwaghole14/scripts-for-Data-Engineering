{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='VISIT_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH visit_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH
        , POST_VISID_HIGH AS POST_VISID_HIGH
        , POST_VISID_LOW AS POST_VISID_LOW
        , IF(CAST(POST_PAGE_EVENT AS INT64) = 0,1,0) AS PAGE_EVENT_COUNT
        , SAFE_CAST(VISIT_NUM AS INT64) AS VISIT_NUM
        , SAFE_CAST(VISIT_START_TIME_GMT AS INT64 ) AS VISIT_START_TIME_GMT
        , SAFE_CAST(VISIT_PAGE_NUM as int64) as VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT as int64) as POST_CUST_HIT_TIME_GMT
        , CONCAT(
          SAFE_CAST(POST_VISID_HIGH AS BIGINT),
          SAFE_CAST(POST_VISID_LOW AS BIGINT),
          SAFE_CAST(VISIT_NUM AS BIGINT),
          SAFE_CAST(VISIT_START_TIME_GMT AS BIGINT)) AS VISIT
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISIT_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, visit_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH
        , POST_VISID_HIGH AS POST_VISID_HIGH
        , POST_VISID_LOW AS POST_VISID_LOW
        , IF(CAST(POST_PAGE_EVENT AS INT64) = 0,1,0) AS PAGE_EVENT_COUNT
        , SAFE_CAST(VISIT_NUM AS INT64) AS VISIT_NUM
        , SAFE_CAST(VISIT_START_TIME_GMT AS INT64 ) AS VISIT_START_TIME_GMT
        , SAFE_CAST(VISIT_PAGE_NUM as int64) as VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT as int64) as POST_CUST_HIT_TIME_GMT
        , CONCAT(
          SAFE_CAST(POST_VISID_HIGH AS BIGINT),
          SAFE_CAST(POST_VISID_LOW AS BIGINT),
          SAFE_CAST(VISIT_NUM AS BIGINT),
          SAFE_CAST(VISIT_START_TIME_GMT AS BIGINT)) AS VISIT
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISIT_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM visit_masthead
    UNION ALL SELECT * FROM visit_replatform
)

, getvisittime AS (

  SELECT *
    , SUM(PAGE_EVENT_COUNT)
      OVER (
        PARTITION BY VISIT_HASH
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS VISIT_PAGE_EVENT_COUNT
    , LAST_VALUE(POST_CUST_HIT_TIME_GMT)
      OVER (
        PARTITION BY VISIT_HASH
        ORDER BY VISIT_PAGE_NUM
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS POST_CUST_HIT_TIME_GMT_LAST
    , SUM(VISIT_PAGE_NUM)
      OVER (
        PARTITION BY VISIT_HASH
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS VISIT_PAGE_NUM_SUM
  FROM merged_data

)

, final AS (

    SELECT VISIT_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(POST_VISID_HIGH) AS POST_VISID_HIGH
    , ANY_VALUE(POST_VISID_LOW) AS POST_VISID_LOW
    , ANY_VALUE(VISIT_NUM) AS VISIT_NUM
    , ANY_VALUE(VISIT_START_TIME_GMT) AS VISIT_START_TIME_GMT
    , ANY_VALUE(TIMESTAMP_SECONDS(POST_CUST_HIT_TIME_GMT_LAST) -
      TIMESTAMP_SECONDS(VISIT_START_TIME_GMT)) AS TIME_SPENT_VISIT
    , CASE WHEN ANY_VALUE(VISIT_PAGE_NUM_SUM) = 1 THEN 1 ELSE 0 END AS IS_BOUNCE
    , CASE WHEN ANY_VALUE(VISIT_PAGE_EVENT_COUNT) = 1 THEN 1 ELSE 0 END AS IS_BOUNCE_PAGE_EVENTS
    , ANY_VALUE(VISIT) AS VISIT
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM getvisittime
    GROUP BY 1 {# deduplicate by grouping on VISIT_KEY #}

)

SELECT

  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY VISIT_HASH) + (SELECT MAX(VISIT_ID) FROM {{ this }}) AS VISIT_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY VISIT_HASH) AS VISIT_ID
  {% endif %}
  , * FROM final
