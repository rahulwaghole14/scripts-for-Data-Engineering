{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='FIRST_HIT_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH first_hit_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as FIRST_HIT_HASH
        , FIRST_HIT_PAGE_URL AS FIRST_HIT_PAGE_URL
        , FIRST_HIT_PAGENAME AS FIRST_HIT_PAGENAME
        , FIRST_HIT_REF_DOMAIN AS FIRST_HIT_REF_DOMAIN
        , CAST(FIRST_HIT_REF_TYPE AS int64) AS FIRST_HIT_REF_TYPE
        , FIRST_HIT_REFERRER AS FIRST_HIT_REFERRER
        , CAST(FIRST_HIT_TIME_GMT AS int64) FIRST_HIT_TIME_GMT
        , POST_CUST_VISID AS POST_CUST_VISID
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT FIRST_HIT_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, first_hit_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as FIRST_HIT_HASH
        , FIRST_HIT_PAGE_URL AS FIRST_HIT_PAGE_URL
        , FIRST_HIT_PAGENAME AS FIRST_HIT_PAGENAME
        , FIRST_HIT_REF_DOMAIN AS FIRST_HIT_REF_DOMAIN
        , CAST(FIRST_HIT_REF_TYPE AS int64) AS FIRST_HIT_REF_TYPE
        , FIRST_HIT_REFERRER AS FIRST_HIT_REFERRER
        , CAST(FIRST_HIT_TIME_GMT AS int64) FIRST_HIT_TIME_GMT
        , POST_CUST_VISID AS POST_CUST_VISID
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT FIRST_HIT_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM first_hit_masthead
    UNION ALL SELECT * FROM first_hit_replatform
)

, final AS (

    SELECT FIRST_HIT_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(FIRST_HIT_PAGE_URL) AS FIRST_HIT_PAGE_URL
    , ANY_VALUE(FIRST_HIT_PAGENAME) AS FIRST_HIT_PAGENAME
    , ANY_VALUE(FIRST_HIT_REF_DOMAIN) AS FIRST_HIT_REF_DOMAIN
    , ANY_VALUE(FIRST_HIT_REF_TYPE) AS FIRST_HIT_REF_TYPE
    , ANY_VALUE(FIRST_HIT_REFERRER) AS FIRST_HIT_REFERRER
    , ANY_VALUE(FIRST_HIT_TIME_GMT) AS FIRST_HIT_TIME_GMT
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on GEO_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY FIRST_HIT_HASH) + (SELECT MAX(FIRST_HIT_ID) FROM {{ this }}) AS FIRST_HIT_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY FIRST_HIT_HASH) AS FIRST_HIT_ID
  {% endif %}
  , * FROM final
