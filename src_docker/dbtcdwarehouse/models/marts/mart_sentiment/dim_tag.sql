{{
  config(
    tags=['sentiment']
  )
}}

WITH tags_data_parsely AS (

    SELECT
        TAG_HASH AS TAG_HASH
        , TAG_CLASS AS TAG_CLASS
        , TAG AS TAG
        , RECORD_SOURCE AS RECORD_SOURCE
    FROM {{ ref('sat_tag_parsely_unnest') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TAG_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, tags_data_composer AS (

    SELECT
        TAG_HASH AS TAG_HASH
        , TAG_CLASS AS TAG_CLASS
        , TAG AS TAG
        , RECORD_SOURCE AS RECORD_SOURCE
    FROM {{ ref('sat_tag_composer_unnest') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TAG_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, tags_data_drupal AS (

    SELECT
        TAG_HASH AS TAG_HASH
        , TAG_CLASS AS TAG_CLASS
        , TAG AS TAG
        , RECORD_SOURCE AS RECORD_SOURCE
    FROM {{ ref('sat_tag_drupal_unnest') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TAG_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, unionall AS (

    SELECT * FROM tags_data_parsely
    UNION ALL SELECT * FROM tags_data_composer
    UNION ALL SELECT * FROM tags_data_drupal

)

SELECT
    -1 AS TAG_ID
    , -1 AS ARTICLE_ID
    -- , '(not set)' as ARTICLE_HASH
    -- , '(not set)' as TAG_HASH
    , '(not set)' AS TAG_CLASS
    , '(not set)' AS TAG
    , '(not set)' AS RECORD_SOURCE
UNION ALL SELECT
    ROW_NUMBER() OVER (ORDER BY unionall.tag_hash) AS TAG_ID
    , ARTICLE.ARTICLE_ID
    -- , ARTICLE.ARTICLE_HASH
    , unionall.TAG_CLASS
    , unionall.TAG
    , unionall.RECORD_SOURCE
FROM unionall
LEFT JOIN {{ ref('link_article_tag_unnest') }} LINK ON unionall.TAG_HASH = LINK.TAG_HASH
INNER JOIN {{ ref('dim_article') }} ARTICLE ON LINK.ARTICLE_HASH = ARTICLE.ARTICLE_HASH
