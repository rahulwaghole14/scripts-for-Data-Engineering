{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='VISITOR_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH visitor_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_HASH
        , POST_VISID_HIGH AS POST_VISID_HIGH
        , POST_VISID_LOW AS POST_VISID_LOW
        , CONCAT(
          SAFE_CAST(POST_VISID_HIGH AS BIGINT),
          SAFE_CAST(POST_VISID_LOW AS BIGINT)) AS VISITOR
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
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, visitor_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_HASH
        , POST_VISID_HIGH AS POST_VISID_HIGH
        , POST_VISID_LOW AS POST_VISID_LOW
        , CONCAT(
          SAFE_CAST(POST_VISID_HIGH AS BIGINT),
          SAFE_CAST(POST_VISID_LOW AS BIGINT)) AS VISITOR
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
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM visitor_masthead
    UNION ALL SELECT * FROM visitor_replatform
)

, final AS (

    SELECT VISITOR_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(POST_VISID_HIGH) AS POST_VISID_HIGH
    , ANY_VALUE(POST_VISID_LOW) AS POST_VISID_LOW
    , ANY_VALUE(VISITOR) AS VISITOR
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on VISIT_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_HASH) + (SELECT MAX(VISITOR_ID) FROM {{ this }}) AS VISITOR_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_HASH) AS VISITOR_ID
  {% endif %}
  , * FROM final
