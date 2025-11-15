-- bigquery
WITH tags AS (

  SELECT
    tag AS TAGS,
    tagType AS TAG_CLASS,
    category AS TAG_CATEGORY,
    record_load_dts AS RECORD_LOAD_DTS,
    record_load_dts AS LOAD_DATE
  FROM
    {{ source('composer', 'composer__tags') }}

),

article_id AS (

  SELECT
    ARTICLE_KEY,
    ARTICLE_CATEGORY
  FROM
    {{ ref('stg_composer__articles') }}

),

split_tags AS (

  SELECT
    TAGS,
    TAG_CLASS,
    TAG_CATEGORY,
    RECORD_LOAD_DTS,
    LOAD_DATE,
    TRIM(tag_part) AS value
  FROM
    tags,
    UNNEST(SPLIT(tags.TAGS, ';')) AS tag_part

)

SELECT
  CAST(article_id.ARTICLE_KEY AS INT64) AS ARTICLE_KEY,
  CONCAT(tags.value, '|', tags.TAG_CLASS) AS TAG_KEY,
  tags.value AS TAG,
  tags.TAG_CLASS,
  tags.RECORD_LOAD_DTS,
  tags.LOAD_DATE
FROM
  split_tags AS tags
LEFT JOIN
  article_id
ON
  article_id.ARTICLE_CATEGORY = tags.TAG_CATEGORY

{# -- sqlserver
-- SELECT
--     ARTICLE_ID.ARTICLE_KEY,
--     CONCAT(s.value, '|', tags.TAG_CLASS) AS TAG_KEY,
--     s.value AS TAG,
--     tags.TAG_CLASS,
--     tags.RECORD_LOAD_DTS,
--     tags.LOAD_DATE
-- FROM
--     (
--     select
--         {{ adapter.quote("tag") }} AS TAGS,
--         {{ adapter.quote("tagType") }} AS TAG_CLASS,
--         {{ adapter.quote("category") }} AS TAG_CATEGORY,
--         {{ adapter.quote("record_load_dts") }} AS RECORD_LOAD_DTS,
--         {{ adapter.quote("record_load_dts") }} AS LOAD_DATE
--     from {{ source('composer', 'tags') }}
--     ) AS tags
-- LEFT JOIN
--     (
--         SELECT ARTICLE_KEY, ARTICLE_CATEGORY
--         FROM {{ ref('stg_composer__articles') }}
--     ) AS ARTICLE_ID
-- ON ARTICLE_ID.ARTICLE_CATEGORY = tags.TAG_CATEGORY
-- CROSS APPLY STRING_SPLIT(tags.TAGS, ';') AS s #}
