{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='VISITOR_USERS_HASH',
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
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR23 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_USERS_HASH
        , POST_EVAR23 AS PROFILE_ID
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR23 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_USERS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, locations_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_USERS_HASH
        , POST_EVAR7 AS PROFILE_ID
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT VISITOR_USERS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM locations_masthead
    UNION ALL SELECT * FROM locations_replatform
)

, final AS (

    SELECT VISITOR_USERS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(PROFILE_ID) AS PROFILE_ID
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on VISITOR_USERS_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_USERS_HASH) + (SELECT MAX(VISITOR_USERS_ID) FROM {{ this }}) AS VISITOR_USERS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY VISITOR_USERS_HASH) AS VISITOR_USERS_ID
  {% endif %}
  , * FROM final
