{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='GEO_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH geo_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_CITY AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as GEO_HASH
        , GEO_COUNTRY AS GEO_COUNTRY
        , GEO_CITY AS GEO_CITY
        , GEO_REGION AS GEO_REGION
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_CITY AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT GEO_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, geo_replatform AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_CITY AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as GEO_HASH
        , GEO_COUNTRY AS GEO_COUNTRY
        , GEO_CITY AS GEO_CITY
        , GEO_REGION AS GEO_REGION
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(GEO_CITY AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT GEO_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM geo_masthead
    UNION ALL SELECT * FROM geo_replatform
)


, final AS (

    SELECT GEO_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(GEO_COUNTRY) AS GEO_COUNTRY
    , ANY_VALUE(GEO_CITY) AS GEO_CITY
    , ANY_VALUE(GEO_REGION) AS GEO_REGION
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on GEO_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY GEO_HASH) + (SELECT MAX(GEO_ID) FROM {{ this }}) AS GEO_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY GEO_HASH) AS GEO_ID
  {% endif %}
  , * FROM final
