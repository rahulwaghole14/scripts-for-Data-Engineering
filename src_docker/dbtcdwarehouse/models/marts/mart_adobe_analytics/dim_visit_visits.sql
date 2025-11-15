{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='VIST_VISITS_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH visits_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING ))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW  AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISID_NEW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP14 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP56 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VIST_VISITS_HASH
        , SAFE_CAST(POST_VISID_HIGH AS int64) AS POST_VISID_HIGH
        , SAFE_CAST(POST_VISID_LOW AS int64)  AS POST_VISID_LOW
        , VISID_NEW AS VISID_NEW
        , SAFE_CAST(VISIT_NUM as int64) AS VISIT_NUM
        , VISIT_REF_DOMAIN AS VISIT_REF_DOMAIN
        , SAFE_CAST(VISIT_REF_TYPE AS int64) AS VISIT_REF_TYPE
        , VISIT_REFERRER AS VISIT_REFERRER
        , VISIT_START_PAGE_URL AS VISIT_START_PAGE_URL
        , VISIT_START_PAGENAME AS VISIT_START_PAGENAME
        , SAFE_CAST(VISIT_START_TIME_GMT AS int64) AS VISIT_START_TIME_GMT
        , SAFE_CAST(POST_PROP14 AS int64) AS MEMBER_ID
        , POST_PROP56 as AUTHENTICATION_STATE
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING ))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW  AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISID_NEW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP14 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP56 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VIST_VISITS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, visits_replatform AS (

      select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING ))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW  AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISID_NEW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR8 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VIST_VISITS_HASH
        , SAFE_CAST(POST_VISID_HIGH AS int64) AS POST_VISID_HIGH
        , SAFE_CAST(POST_VISID_LOW AS int64)  AS POST_VISID_LOW
        , VISID_NEW AS VISID_NEW
        , SAFE_CAST(VISIT_NUM as int64) AS VISIT_NUM
        , VISIT_REF_DOMAIN AS VISIT_REF_DOMAIN
        , SAFE_CAST(VISIT_REF_TYPE AS int64) AS VISIT_REF_TYPE
        , VISIT_REFERRER AS VISIT_REFERRER
        , VISIT_START_PAGE_URL AS VISIT_START_PAGE_URL
        , VISIT_START_PAGENAME AS VISIT_START_PAGENAME
        , SAFE_CAST(VISIT_START_TIME_GMT AS int64) AS VISIT_START_TIME_GMT
        , SAFE_CAST(POST_EVAR7 AS int64) AS MEMBER_ID
        , POST_EVAR8 as AUTHENTICATION_STATE
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING ))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW  AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISID_NEW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR8 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VIST_VISITS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM visits_masthead
    UNION ALL SELECT * FROM visits_replatform
)

, final AS (

    SELECT VIST_VISITS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(POST_VISID_HIGH) AS POST_VISID_HIGH
    , ANY_VALUE(POST_VISID_LOW ) AS POST_VISID_LOW
    , ANY_VALUE(VISID_NEW) AS VISID_NEW
    , ANY_VALUE(VISIT_NUM) AS VISIT_NUM
    , ANY_VALUE(VISIT_REF_DOMAIN) AS VISIT_REF_DOMAIN
    , ANY_VALUE(VISIT_REF_TYPE) AS VISIT_REF_TYPE
    , ANY_VALUE(VISIT_REFERRER) AS VISIT_REFERRER
    , ANY_VALUE(VISIT_START_PAGE_URL) AS VISIT_START_PAGE_URL
    , ANY_VALUE(VISIT_START_PAGENAME) AS VISIT_START_PAGENAME
    , ANY_VALUE(VISIT_START_TIME_GMT) AS VISIT_START_TIME_GMT
    , ANY_VALUE(MEMBER_ID) AS MEMBER_ID
    , ANY_VALUE(AUTHENTICATION_STATE) AS AUTHENTICATION_STATE
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on VIST_VISITS_ID #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY VIST_VISITS_HASH) + (SELECT MAX(VIST_VISITS_ID) FROM {{ this }}) AS VIST_VISITS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY VIST_VISITS_HASH) AS VIST_VISITS_ID
  {% endif %}
  , * FROM final
