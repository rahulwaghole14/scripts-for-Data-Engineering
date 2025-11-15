{{ config(
    tags=['adobe'],
    materialized='table',
    on_schema_change='sync_all_columns'
) }}


WITH top_articles AS (
    SELECT
        DATE(LOAD_DATE) AS day
        , POST_PAGENAME
        , PAGE_URL
        , COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS unique_visitors
    FROM {{ ref('sat_adobe_analytics_masthead_replatform') }}
    WHERE
        DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE(LOAD_DATE) < CURRENT_DATE('Pacific/Auckland')
        AND LOWER(GEO_COUNTRY) = 'nzl'
        AND POST_PAGENAME IS NOT NULL
        AND POST_PAGENAME != 'home'
    GROUP BY day, POST_PAGENAME, PAGE_URL
)

, RankedArticles AS (
    SELECT
        day
        , POST_PAGENAME
        , unique_visitors
        , PAGE_URL
        , RANK() OVER (PARTITION BY day ORDER BY unique_visitors DESC) AS rank
    FROM top_articles
    QUALIFY rank <= 10
)

SELECT
    day
    , POST_PAGENAME
    , unique_visitors
    , PAGE_URL
FROM RankedArticles
ORDER BY day DESC, rank
