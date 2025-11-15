{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='HIT_SITE_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH hit_sites_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP1 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR163 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SITE_HASH
        , POST_PROP1 AS SITE_1
        , POST_EVAR163 AS MASTHEAD
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP1 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR163 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_SITE_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, hit_sites_replatform AS (

   select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST('(not set)' AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SITE_HASH
        , '(not set)' AS SITE_1
        , POST_EVAR25 AS MASTHEAD
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST('(not set)' AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_SITE_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM hit_sites_masthead
    UNION ALL SELECT * FROM hit_sites_replatform
)

, final AS (

    SELECT HIT_SITE_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(SITE_1) AS SITE_1
    , ANY_VALUE(MASTHEAD) AS MASTHEAD
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on GEO_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY HIT_SITE_HASH) + (SELECT MAX(HIT_SITE_ID) FROM {{ this }}) AS HIT_SITE_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY HIT_SITE_HASH) AS HIT_SITE_ID
  {% endif %}
  , * FROM final
