{{
  config(
    tags=['sentiment']
  )
}}

WITH hub AS (

    SELECT
        hub.RESPONSE_HASH
        , sat.RESPONSE_RESPONSEID
        , sat.RESPONSE_RECORDEDDATE
        , sat.RESPONSE_REGION_NAME
        , sat.RESPONSE_hexa_ACCOUNT_ID
    FROM {{ ref('hub_question_response') }} hub
    LEFT JOIN {{ ref('sat_question_response') }} sat
        ON hub.RESPONSE_HASH = sat.RESPONSE_HASH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sat.RESPONSE_HASH ORDER BY sat.EFFECTIVE_FROM DESC) = 1

)

, webevents AS (

    SELECT

        ROW_NUMBER() OVER (ORDER BY hub.RESPONSE_HASH) AS FACT_ID
        , COALESCE(dim_article.ARTICLE_ID, -1) AS ARTICLE_ID
        -- , COALESCE(dim_tag.TAG_ID, -1) AS TAG_ID
        , COALESCE(dim_sentiment.SENTIMENT_ID, -1) AS SENTIMENT_ID
        , COALESCE(hub.RESPONSE_hexa_ACCOUNT_ID, -1) AS ACCOUNT_ID
        , MAX(COALESCE(dim_sentiment_why.SENTIMENT_WHY_ID, -1)) OVER (PARTITION BY hub.RESPONSE_RESPONSEID) AS SENTIMENT_WHY_ID
        , COALESCE(dim_region.REGION_ID, -1) AS REGION_ID
        -- , 1 as QUESTION_RESPONSE_COUNT
        , ROW_NUMBER() OVER (PARTITION BY dim_sentiment.RESPONSE_ID ORDER BY hub.RESPONSE_HASH) AS SENTIMENT_ROW_NUMBER
        , COALESCE(second_dim_sentiment.SENTIMENT_ID, -1) AS SECOND_SENTIMENT_ID
        , CASE WHEN COALESCE(second_dim_sentiment.SENTIMENT_ID, -1) != -1 THEN 1 ELSE 0 END AS HAS_MULTIPLE_SENTIMENTS
        , FORMAT_TIMESTAMP(
            "%Y-%m-%dT%H:%M:%S"
            , TIMESTAMP(DATETIME(hub.RESPONSE_RECORDEDDATE), "UTC")
            , "Pacific/Auckland"
        ) AS RECORDED_DATE

    -- central hub granularity
    FROM hub

    -- add links to get hashes
    LEFT JOIN {{ ref('link_article_question_response') }} link_article_question_response
        ON hub.RESPONSE_HASH = link_article_question_response.RESPONSE_HASH
    {# -- LEFT JOIN {{ ref('link_article_tag_unnest') }} link_article_tag
    -- ON link_article_tag.ARTICLE_HASH = link_article_question_response.ARTICLE_HASH #}

    -- add dims to get IDs
    LEFT JOIN {{ ref('dim_article') }} dim_article
        ON link_article_question_response.ARTICLE_HASH = dim_article.ARTICLE_HASH
    {# -- LEFT JOIN {{ ref('dim_tag') }} dim_tag
    -- ON dim_tag.TAG_HASH = link_article_tag.TAG_HASH #}
    LEFT JOIN {{ ref('dim_sentiment') }} dim_sentiment
        ON hub.RESPONSE_HASH = dim_sentiment.RESPONSE_HASH
    LEFT JOIN {{ ref('dim_sentiment') }} second_dim_sentiment
        ON dim_sentiment.RESPONSE_ID = second_dim_sentiment.RESPONSE_ID AND dim_sentiment.RESPONSE_HASH != second_dim_sentiment.RESPONSE_HASH
    LEFT JOIN {{ ref('dim_sentiment_why') }} dim_sentiment_why
        ON hub.RESPONSE_HASH = dim_sentiment_why.RESPONSE_HASH
    LEFT JOIN {{ ref('dim_region') }} dim_region
        ON dim_region.REGION = CAST(LOWER(COALESCE(hub.RESPONSE_REGION_NAME, '(not set)')) AS STRING)

)

SELECT * FROM webevents
WHERE SENTIMENT_ID != -1
