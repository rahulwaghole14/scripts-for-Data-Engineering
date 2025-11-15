WITH source AS (
  select * from {{ source('composer', 'composer__articles') }}
),
renamed AS (
  SELECT
    CAST(content_id AS INT64) AS ARTICLE_KEY,
    title AS ARTICLE_TITLE,
    published_dts AS ARTICLE_PUBLISHED_DTS,
    TO_JSON_STRING(source) AS ARTICLE_SOURCE,  -- Updated this line

    {# -- CAST(source AS STRING) AS ARTICLE_SOURCE,  -- Cast here #}

    brand AS ARTICLE_BRAND,
    category AS ARTICLE_CATEGORY,
    category_1 AS ARTICLE_CATEGORY_1,
    category_2 AS ARTICLE_CATEGORY_2,
    category_3 AS ARTICLE_CATEGORY_3,
    category_4 AS ARTICLE_CATEGORY_4,
    category_5 AS ARTICLE_CATEGORY_5,
    category_6 AS ARTICLE_CATEGORY_6,
    print_slug AS ARTICLE_PRINT_SLUG,
    author AS ARTICLE_AUTHOR,
    word_count AS ARTICLE_WORD_COUNT,
    image_count AS ARTICLE_IMAGE_COUNT,
    video_count AS ARTICLE_VIDEO_COUNT,
    advertisement AS ARTICLE_ADVERTISEMENT,
    sponsored AS ARTICLE_SPONSORED,
    promoted_flag AS ARTICLE_PROMOTED_FLAG,
    comments_flag AS ARTICLE_COMMENTS_FLAG,
    home_flag AS ARTICLE_HOME_FLAG,
    record_load_dts_utc AS RECORD_LOAD_DTS_UTC,
    load_dt AS LOAD_DATETIME
  FROM source
)

SELECT * FROM renamed
