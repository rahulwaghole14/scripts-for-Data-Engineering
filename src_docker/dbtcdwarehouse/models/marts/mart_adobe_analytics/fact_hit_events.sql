{% set partitions_to_replace = [
  'DATE(CURRENT_DATE("Pacific/Auckland"))',
  'DATE(date_sub(CURRENT_DATE("Pacific/Auckland"), interval 1 day))',
  'DATE(date_sub(CURRENT_DATE("Pacific/Auckland"), interval 2 day))'
] %}

{# {% set partitions_to_replace = [
  'DATE("2023-11-11")','DATE("2023-11-12")','DATE("2023-11-13")'
] %}  #}

{{
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='sync_all_columns',
    tags = ['adobe'],
    partition_by = {"field": "LOAD_DATE", "data_type": "timestamp", "granularity": "day"},
    partitions = partitions_to_replace
  )
}}

WITH hub_masthead as (

    SELECT sat.ADOBE_HASH
    , sat.ADOBE_KEY
    , sat.LOAD_DATE
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_CITY AS STRING))), ''), '^^'), '||'
    )))) AS STRING) as GEO_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as FIRST_HIT_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR73 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP14 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP56 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_HITS_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(referrer AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as REF_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
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
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP1 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR163 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SITE_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_PROP25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_PLATFORMS_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
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
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR84 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR143 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_LINKS_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_VISITORS_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR23 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_USERS_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_HASH
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SERVERS_HASH
    , CASE WHEN CAST(POST_PAGE_EVENT AS INT64) IN (100,101,102) THEN 1
      ELSE 0 END AS IS_LINK_TRACKING
    FROM {{ ref('sat_adobe_analytics_masthead') }} sat
    {# add condition on incremental updates #}
    {% if is_incremental() and target.dataset == 'prod' %}
        {# -- Incremental in production #}
        WHERE DATE(sat.LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
        AND CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% elif is_incremental() and target.dataset != 'prod' %}
        WHERE DATE(sat.LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
        AND CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% else %}
        {# -- Full refresh in production or other conditions #}
        {# -- No WHERE clause or a different condition as needed #}
        WHERE CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sat.ADOBE_HASH ORDER BY sat.EFFECTIVE_FROM DESC) = 1

)

, hub_replatform AS (

    SELECT sat.ADOBE_HASH
    , sat.ADOBE_KEY
    , sat.LOAD_DATE
    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_REGION AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_COUNTRY AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.GEO_CITY AS STRING))), ''), '^^'), '||'
    )))) AS STRING) as GEO_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGE_URL AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_PAGENAME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_REFERRER AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(FIRST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_CUST_VISID AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as FIRST_HIT_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(HITID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(LAST_HIT_TIME_GMT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(NEW_VISIT AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_PAGE_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(DATE_TIME AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(PREV_PAGE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR8 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_HITS_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(REF_DOMAIN AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(REF_TYPE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(referrer AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as REF_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
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

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST('(not set)' AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR25 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SITE_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MOBLIE_ID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR28 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_PLATFORMS_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
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

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR11 AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR11 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_LINKS_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MCVISID AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(MONTHLY_VISITOR AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_VISITORS_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_EVAR7 AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_USERS_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_NUM AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(VISIT_START_TIME_GMT AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISIT_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_HIGH AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_VISID_LOW AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as VISITOR_HASH

    , CAST(UPPER(TO_HEX(MD5(CONCAT(
            IFNULL(NULLIF(UPPER(TRIM(CAST(sat.RECORD_SOURCE AS STRING))), ''), '^^'), '||',
            IFNULL(NULLIF(UPPER(TRIM(CAST(POST_USER_SERVER AS STRING))), ''), '^^'), '||'
        )))) AS STRING) as HIT_SERVERS_HASH
    , CASE WHEN CAST(POST_PAGE_EVENT AS INT64) IN (100,101,102) THEN 1
      ELSE 0 END AS IS_LINK_TRACKING
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }} sat
    {# add condition on incremental updates #}
    {% if is_incremental() and target.dataset == 'prod' %}
        {# -- Incremental in production #}
        WHERE DATE(sat.LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
        AND CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% elif is_incremental() and target.dataset != 'prod' %}
        WHERE DATE(sat.LOAD_DATE) IN ({{ partitions_to_replace | join(',') }})
        AND CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% else %}
        {# -- Full refresh in production or other conditions #}
        {# -- No WHERE clause or a different condition as needed #}
        WHERE CAST( sat.POST_PAGE_EVENT AS INT ) != 0
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY sat.ADOBE_HASH ORDER BY sat.EFFECTIVE_FROM DESC) = 1

)

, merged_data AS (
    SELECT * FROM hub_masthead
    UNION ALL SELECT * FROM hub_replatform
)

, webevents AS (

    SELECT
      {% if is_incremental() %}
        ROW_NUMBER() OVER ( ORDER BY merged_data.ADOBE_KEY )
        + (SELECT MAX(FACT_ID) FROM {{ this }}) AS FACT_ID
      {% else %}
        ROW_NUMBER() OVER ( ORDER BY merged_data.ADOBE_KEY ) AS FACT_ID
      {% endif %}
      , dim_hit_geolocations.GEO_ID
      , dim_visitor_first_hits.FIRST_HIT_ID
      , dim_hit_hits.HIT_HITS_ID
      , dim_hit_referrers.REF_ID
      , dim_visit_visits.VIST_VISITS_ID
      , dim_hit_sites.HIT_SITE_ID
      , dim_hit_platforms.HIT_PLATFORMS_ID
      , dim_hit_articles.HIT_ARTICLES_ID
      , dim_hit_links.HIT_LINKS_ID
      , dim_visitor_visitors.VISITOR_VISITORS_ID
      , dim_visitor_users.VISITOR_USERS_ID
      , dim_visit_cd.VISIT_ID
      , dim_visitor_cd.VISITOR_ID
      , dim_hit_servers.HIT_SERVERS_ID
      , merged_data.IS_LINK_TRACKING
      , merged_data.LOAD_DATE


    -- central hub granularity
    FROM merged_data

    -- join dimension tables
    LEFT JOIN {{ ref('dim_hit_geolocations') }} dim_hit_geolocations USING (GEO_HASH)
    LEFT JOIN {{ ref('dim_visitor_first_hits') }} dim_visitor_first_hits USING (FIRST_HIT_HASH)
    LEFT JOIN {{ ref('dim_hit_hits') }} dim_hit_hits USING (HIT_HITS_HASH)
    LEFT JOIN {{ ref('dim_hit_referrers') }} dim_hit_referrers USING (REF_HASH)
    LEFT JOIN {{ ref('dim_visit_visits') }} dim_visit_visits USING (VIST_VISITS_HASH)
    LEFT JOIN {{ ref('dim_hit_sites') }} dim_hit_sites USING (HIT_SITE_HASH)
    LEFT JOIN {{ ref('dim_hit_platforms') }} dim_hit_platforms USING (HIT_PLATFORMS_HASH)
    LEFT JOIN {{ ref('dim_hit_articles') }} dim_hit_articles USING (HIT_ARTICLES_HASH)
    LEFT JOIN {{ ref('dim_hit_links') }} dim_hit_links USING (HIT_LINKS_HASH)
    LEFT JOIN {{ ref('dim_visitor_visitors') }} dim_visitor_visitors USING (VISITOR_VISITORS_HASH)
    LEFT JOIN {{ ref('dim_visitor_users') }} dim_visitor_users USING (VISITOR_USERS_HASH)
    LEFT JOIN {{ ref('dim_visit_cd') }} dim_visit_cd USING (VISIT_HASH)
    LEFT JOIN {{ ref('dim_visitor_cd') }} dim_visitor_cd USING (VISITOR_HASH)
    LEFT JOIN {{ ref('dim_hit_servers') }} dim_hit_servers USING (HIT_SERVERS_HASH)

)

SELECT * FROM webevents
