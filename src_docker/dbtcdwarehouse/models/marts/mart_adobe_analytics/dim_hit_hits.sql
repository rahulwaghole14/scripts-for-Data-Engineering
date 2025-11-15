{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='HIT_HITS_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH pages AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR73 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP14 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP56 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_HITS_HASH
        , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH
        , SAFE_CAST(POST_PAGE_EVENT AS INT64) as POST_PAGE_EVENT
        , SAFE_CAST(VISIT_PAGE_NUM as int64) as VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT as int64) AS POST_CUST_HIT_TIME_GMT
        , POST_EVAR73 AS PREVIOUS_PAGE
        , SAFE_CAST(DATE_TIME AS datetime) AS DATE_TIME
        , SAFE_CAST(HITID_HIGH AS int64) AS HITID_HIGH
        , SAFE_CAST(HITID_LOW AS int64) AS HITID_LOW
        , SAFE_CAST(NEW_VISIT as int64) AS NEW_VISIT
        , SAFE_CAST(LAST_HIT_TIME_GMT AS int64) AS LAST_HIT_TIME_GMT
        , SAFE_CAST(POST_PROP14 AS int64) AS MEMBER_ID
        , POST_PROP56 AS AUTHENTICATION_STATE
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR73 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP14 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP56 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_HITS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}
)

, pages_replatform AS (

   select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(PREV_PAGE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR8 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_HITS_HASH
        , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH
        , SAFE_CAST(POST_PAGE_EVENT AS INT64) as POST_PAGE_EVENT
        , SAFE_CAST(VISIT_PAGE_NUM as int64) as VISIT_PAGE_NUM
        , SAFE_CAST(POST_CUST_HIT_TIME_GMT as int64) AS POST_CUST_HIT_TIME_GMT
        , PREV_PAGE AS PREVIOUS_PAGE
        , SAFE_CAST(DATE_TIME AS datetime) AS DATE_TIME
        , SAFE_CAST(HITID_HIGH AS int64) AS HITID_HIGH
        , SAFE_CAST(HITID_LOW AS int64) AS HITID_LOW
        , SAFE_CAST(NEW_VISIT as int64) AS NEW_VISIT
        , SAFE_CAST(LAST_HIT_TIME_GMT AS int64) AS LAST_HIT_TIME_GMT
        , SAFE_CAST(POST_EVAR7 AS int64) AS MEMBER_ID
        , POST_EVAR8 AS AUTHENTICATION_STATE
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(PREV_PAGE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR8 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_HITS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM pages
    UNION ALL SELECT * FROM pages_replatform
)

, timeonpage AS (

    SELECT *
      , LAST_VALUE(POST_CUST_HIT_TIME_GMT) OVER ( PARTITION BY POST_PAGE_EVENT, VISIT_HASH ORDER BY VISIT_PAGE_NUM
        ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING ) AS NEXT_PAGE_HIT_TIME_GMT
      , LAST_VALUE(POST_CUST_HIT_TIME_GMT) OVER ( PARTITION BY VISIT_HASH ORDER BY VISIT_PAGE_NUM
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS LAST_EVENT_HIT_TIME_GMT
      , CASE WHEN VISIT_PAGE_NUM = LAST_VALUE(VISIT_PAGE_NUM) OVER ( PARTITION BY POST_PAGE_EVENT, VISIT_HASH ORDER BY VISIT_PAGE_NUM
        ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING ) THEN 1 ELSE 0 END AS IS_LAST_EVENT_TYPE_HIT
    FROM merged_data

)

, calctimeonpage AS (

    SELECT *
      , case WHEN IS_LAST_EVENT_TYPE_HIT = 1 AND POST_PAGE_EVENT = 0 THEN
          ( TIMESTAMP_SECONDS(LAST_EVENT_HIT_TIME_GMT) - TIMESTAMP_SECONDS(POST_CUST_HIT_TIME_GMT))
          WHEN POST_PAGE_EVENT = 0 THEN
          ( TIMESTAMP_SECONDS(NEXT_PAGE_HIT_TIME_GMT) - TIMESTAMP_SECONDS(POST_CUST_HIT_TIME_GMT))
          ELSE NULL end AS TIME_SPENT_PAGE
    FROM timeonpage

)

, final AS (

    SELECT HIT_HITS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(PREVIOUS_PAGE) AS PREVIOUS_PAGE
    , ANY_VALUE(HITID_HIGH) AS HITID_HIGH
    , ANY_VALUE(HITID_LOW) AS HITID_LOW
    , ANY_VALUE(LAST_HIT_TIME_GMT) AS LAST_HIT_TIME_GMT
    , ANY_VALUE(NEW_VISIT) AS NEW_VISIT
    , ANY_VALUE(MEMBER_ID) AS MEMBER_ID
    , ANY_VALUE(VISIT_PAGE_NUM) AS VISIT_PAGE_NUM
    , ANY_VALUE(AUTHENTICATION_STATE) AS AUTHENTICATION_STATE
    , ANY_VALUE(DATE_TIME) AS DATE_TIME
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    , ANY_VALUE(TIME_SPENT_PAGE) AS TIME_SPENT_PAGE
    FROM calctimeonpage
    GROUP BY 1

)


SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY HIT_HITS_HASH) + (SELECT MAX(HIT_HITS_ID) FROM {{ this }}) AS HIT_HITS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY HIT_HITS_HASH) AS HIT_HITS_ID
  {% endif %}
 , * FROM final
