{% set partitions_to_replace = [
  'DATE(CURRENT_DATE("Pacific/Auckland"))',
  'DATE(DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 DAY))',
  'DATE(DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 2 DAY))'
] -%}

{{-
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change='sync_all_columns',
    tags = ['adobe'],
    partition_by = {"field": "read_date", "data_type": "date", "granularity": "day"},
    partitions = partitions_to_replace
  )
-}}

WITH masthead_logins AS (

    SELECT
        sat.post_evar14
        , sat.POST_EVAR163
        , DATE(sat.load_date) as read_date
        , sat.POST_VISID_HIGH
        , sat.POST_VISID_LOW
        , sat.page_url
    FROM {{ ref('sat_adobe_analytics_masthead') }} sat

    {%- if is_incremental() %}
        {# -- Incremental in production #}
    WHERE DATE(sat.LOAD_DATE) IN ({{ partitions_to_replace | join(', ') }})
    {%- else -%}
        {# -- Full refresh in production or other conditions #}
    WHERE DATE(hub.LOAD_DATE) >= '2023-10-09'
    {%- endif %}
        AND sat.geo_country = 'nzl'
        AND sat.POST_EVAR163 IS NOT NULL
        AND CAST( sat.POST_PAGE_EVENT AS INT ) = 0

)

SELECT
    post_evar14
    , POST_EVAR163
    , read_date
    , count(*) AS page_views
    , count(distinct concat(POST_VISID_HIGH, POST_VISID_LOW)) AS unique_visitor
    , count(case when  REGEXP_CONTAINS(page_url, r"[0-9]{9}") = true then 1 end) as page_views_articles
FROM masthead_logins
GROUP BY 1, 2, 3
