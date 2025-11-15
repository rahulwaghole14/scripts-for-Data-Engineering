{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='HIT_ARTICLES_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH pages_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP2 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP3 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP4 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP8 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP11 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP28 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP31 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP47 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP54 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP61 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP64 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_ARTICLES_HASH
        , POST_PROP2 AS SITE_SECTION
        , POST_PROP3 AS PAGE_TITLE
        , POST_PROP4 AS FRIENDLY_PAGE_NAME
        , POST_PROP8 AS SUB_SECTION
        , POST_PROP11 AS CONTENT_ID
        , POST_PROP28 AS PAGE_NAME
        , POST_PROP31 AS PAGE_URL
        , POST_PROP47 AS CONTENT_TYPE
        , POST_PROP54 AS TITLE
        , POST_PROP61 AS CONTENT_ID_AND_TITLE
        , POST_PROP64 AS AUTHOR
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP2 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP3 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP4 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP8 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP11 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP28 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP31 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP47 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP54 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP61 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP64 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_ARTICLES_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}
)

, pages_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR4 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR21 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR6 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP35 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR12 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP39 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP22 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR29 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_ARTICLES_HASH
        , POST_EVAR4 AS SITE_SECTION
        , POST_PROP5 AS PAGE_TITLE
        , POST_PROP5 AS FRIENDLY_PAGE_NAME
        , POST_EVAR21 AS SUB_SECTION
        , POST_EVAR6 AS CONTENT_ID
        , POST_PROP5 AS PAGE_NAME
        , POST_PROP35 AS PAGE_URL
        , POST_EVAR12 AS CONTENT_TYPE
        , POST_PROP39 AS TITLE
        , POST_PROP22 AS CONTENT_ID_AND_TITLE
        , POST_EVAR29 AS AUTHOR
        , RECORD_SOURCE
        , LOAD_DATE AS LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR4 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR21 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR6 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP5 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP35 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR12 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP39 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP22 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR29 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_ARTICLES_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}
)

, merged_data AS (
    SELECT * FROM pages_masthead
    UNION ALL SELECT * FROM pages_replatform
)

, final AS (

    SELECT HIT_ARTICLES_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(SITE_SECTION) AS SITE_SECTION
    , ANY_VALUE(PAGE_TITLE) AS PAGE_TITLE
    , ANY_VALUE(FRIENDLY_PAGE_NAME) AS FRIENDLY_PAGE_NAME
    , ANY_VALUE(CONTENT_ID) AS CONTENT_ID
    , ANY_VALUE(PAGE_NAME) AS PAGE_NAME
    , ANY_VALUE(PAGE_URL) AS PAGE_URL
    , ANY_VALUE(CONTENT_TYPE) AS CONTENT_TYPE
    , ANY_VALUE(TITLE) AS TITLE
    , ANY_VALUE(CONTENT_ID_AND_TITLE) AS CONTENT_ID_AND_TITLE
    , ANY_VALUE(AUTHOR) AS AUTHOR
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on GEO_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY HIT_ARTICLES_HASH) + (SELECT MAX(HIT_ARTICLES_ID) FROM {{ this }}) AS HIT_ARTICLES_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY HIT_ARTICLES_HASH) AS HIT_ARTICLES_ID
  {% endif %}
  , * FROM final
