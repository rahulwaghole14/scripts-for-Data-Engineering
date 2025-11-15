{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='VISITOR_VISITORS_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH locations_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_VISITORS_HASH
        , MCVISID AS MCVISID
        , CAST(MONTHLY_VISITOR AS int64) AS MONTHLY_VISITOR
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_VISITORS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, locations_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_VISITORS_HASH
        , MCVISID AS MCVISID
        , CAST(MONTHLY_VISITOR AS int64) AS MONTHLY_VISITOR
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_VISITORS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM locations_masthead
    UNION ALL SELECT * FROM locations_replatform
)

, final AS (

    SELECT VISITOR_VISITORS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(MCVISID) AS MCVISID
    , ANY_VALUE(MONTHLY_VISITOR) AS MONTHLY_VISITOR
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on VISITOR_VISITORS_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_VISITORS_HASH) + (SELECT MAX(VISITOR_VISITORS_ID) FROM {{ this }}) AS VISITOR_VISITORS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_VISITORS_HASH) AS VISITOR_VISITORS_ID
  {% endif %}
  , * FROM final
