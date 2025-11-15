{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='HIT_PLATFORMS_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH hit_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_PLATFORMS_HASH
        , SAFE_CAST(MOBLIE_ID AS int64) AS  MOBILE_ID
        , POST_PROP25 AS ENVIRONMENT
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_PLATFORMS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, hit_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR28 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_PLATFORMS_HASH
        , SAFE_CAST(MOBLIE_ID AS int64) AS  MOBILE_ID
        , POST_EVAR28 AS ENVIRONMENT
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR28 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_PLATFORMS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM hit_masthead
    UNION ALL SELECT * FROM hit_replatform
)

, final AS (

    SELECT HIT_PLATFORMS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(MOBILE_ID) AS MOBILE_ID
    , ANY_VALUE(ENVIRONMENT) AS ENVIRONMENT
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on HIT_PLATFORMS_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY HIT_PLATFORMS_HASH) + (SELECT MAX(HIT_PLATFORMS_ID) FROM {{ this }}) AS HIT_PLATFORMS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY HIT_PLATFORMS_HASH) AS HIT_PLATFORMS_ID
  {% endif %}
  , * FROM final
