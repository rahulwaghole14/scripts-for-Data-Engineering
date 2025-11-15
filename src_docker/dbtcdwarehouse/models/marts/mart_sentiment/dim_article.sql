{{
  config(
    tags=['sentiment']
  )
}}

WITH articles_cq AS (

    SELECT
    DISTINCT
        ARTICLE_KEY AS ARTICLE_ID
        , ARTICLE_KEY AS ARTICLE_KEY
        , ARTICLE_HASH
        , ARTICLE_TITLE AS TITLE
        , '(not set)' AS BYLINE
        , ARTICLE_AUTHOR AS AUTHOR
        , '(not set)' AS AUTHOR_ID
        , ARTICLE_PUBLISHED_DTS AS PUBLISHED_DTS
        , RECORD_SOURCE
    FROM {{ ref('sat_article') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ARTICLE_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, articles_drupal AS (

    SELECT
    DISTINCT
        ARTICLE_KEY AS ARTICLE_ID
        , ARTICLE_KEY AS ARTICLE_KEY
        , ARTICLE_HASH
        , ARTICLE_HEADLINE AS TITLE
        , ARTICLE_BYLINE AS BYLINE
        , ARTICLE_AUTHOR_NAME AS AUTHOR
        , ARTICLE_AUTHOR_ID AS AUTHOR_ID
        , SAFE_CAST(ARTICLE_PUBLISHEDDATE AS TIMESTAMP) AS PUBLISHED_DTS
        , RECORD_SOURCE
    FROM {{ ref('sat_article_drupal') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ARTICLE_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, merge_cq_drupal AS (

    SELECT articles_cq.*
    FROM articles_cq
    LEFT JOIN articles_drupal USING (ARTICLE_KEY)
    WHERE articles_drupal.ARTICLE_KEY IS NULL
    UNION ALL
    SELECT * FROM articles_drupal

)


SELECT
    -1 AS ARTICLE_ID
    , -1 AS ARTICLE_KEY
    , '(not set)' AS ARTICLE_HASH
    , '(not set)' AS TITLE
    , '(not set)' AS BYLINE
    , '(not set)' AS AUTHOR
    , '(not set)' AS AUTHOR_ID
    , '1970-01-01 00:00:00.000' AS PUBLISHED_DTS
    , '(not set)' AS RECORD_SOURCE
UNION ALL
SELECT * FROM merge_cq_drupal
