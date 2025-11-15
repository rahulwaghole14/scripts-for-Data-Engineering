{{
  config(
    tags=['adobe'],
  )
}}

WITH sponsored_content AS (

    SELECT
        ARTICLE_ID
        , ARTICLE_TYPEOFWORKLABEL_AGG
        , ARTICLE_CREATEDDATE
        , ARTICLE_HEADLINE
        , RECORD_SOURCE
        , LOAD_DATE
    FROM {{ ref('sat_article_drupal') }}
    WHERE
        ('SPONSORED' IN (ARTICLE_TYPEOFWORKLABEL_AGG) OR 'BRAND CONTENT' IN (ARTICLE_TYPEOFWORKLABEL_AGG))


)

SELECT * FROM sponsored_content
