{{
  config(
    tags=['adobe'],
    materialized='incremental',
    unique_key='HIT_SERVERS_HASH',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH server_masthead AS (

    select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SERVERS_HASH
        , POST_USER_SERVER AS SERVER_DIM
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_SERVERS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, server_replatform AS (

   select
        CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SERVERS_HASH
        , POST_USER_SERVER AS SERVER_DIM
        , RECORD_SOURCE
        , LOAD_DATE as LOAD_DATE
    from {{ ref('sat_adobe_analytics_masthead_replatform') }}

    {% if is_incremental() %} {#  evaluates to true for incremental models #}
    WHERE LOAD_DATE > COALESCE(
        (SELECT MAX(LOAD_DATE) from {{ this }}), '1900-01-01'
        )
    AND CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) NOT IN (SELECT HIT_SERVERS_HASH FROM {{ this }})
    {% else %}
      {# do not add condition if not incremental #}
    {% endif %}

)

, merged_data AS (
    SELECT * FROM server_masthead
    UNION ALL SELECT * FROM server_replatform
)

, final AS (

    SELECT HIT_SERVERS_HASH
    , ANY_VALUE(RECORD_SOURCE) AS RECORD_SOURCE
    , ANY_VALUE(SERVER_DIM) AS SERVER_DIM
    , ANY_VALUE(LOAD_DATE) AS LOAD_DATE
    FROM merged_data
    GROUP BY 1 {# deduplicate by grouping on HIT_SERVERS_KEY #}

)

SELECT
  {% if is_incremental() %}
  ROW_NUMBER() OVER (ORDER BY HIT_SERVERS_HASH) + (SELECT MAX(HIT_SERVERS_ID) FROM {{ this }}) AS HIT_SERVERS_ID
  {% else %}
  ROW_NUMBER() OVER (ORDER BY HIT_SERVERS_HASH) AS HIT_SERVERS_ID
  {% endif %}
  , * FROM final
