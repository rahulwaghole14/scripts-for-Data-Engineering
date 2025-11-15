{%- set yaml_metadata -%}
depends_on: stg_composer__articles
source_model: stg_composer__articles
derived_columns:
  RECORD_SOURCE: "!COMPOSER_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS_UTC
  EFFECTIVE_FROM: LOAD_DATETIME
hashed_columns:
  ARTICLE_HASH: "ARTICLE_KEY"
  ARTICLE_HASHDIFF:
    is_hashdiff: true
    columns:
      - ARTICLE_KEY
      - ARTICLE_TITLE
      - ARTICLE_PUBLISHED_DTS
      - ARTICLE_SOURCE
      - ARTICLE_BRAND
      - ARTICLE_CATEGORY
      - ARTICLE_CATEGORY_1
      - ARTICLE_CATEGORY_2
      - ARTICLE_CATEGORY_3
      - ARTICLE_CATEGORY_4
      - ARTICLE_CATEGORY_5
      - ARTICLE_CATEGORY_6
      - ARTICLE_PRINT_SLUG
      - ARTICLE_AUTHOR
      - ARTICLE_WORD_COUNT
      - ARTICLE_IMAGE_COUNT
      - ARTICLE_VIDEO_COUNT
      - ARTICLE_ADVERTISEMENT
      - ARTICLE_SPONSORED
      - ARTICLE_PROMOTED_FLAG
      - ARTICLE_COMMENTS_FLAG
      - ARTICLE_HOME_FLAG
{%- endset -%}
{%- set metadata_dict = fromyaml(yaml_metadata) -%}
{%- set source_model = metadata_dict['source_model'] -%}
{%- set derived_columns = metadata_dict['derived_columns'] -%}
{%- set hashed_columns = metadata_dict['hashed_columns'] -%}
{{ automate_dv.stage(
  include_source_columns=true,
  source_model=source_model,
  derived_columns=derived_columns,
  hashed_columns=hashed_columns
  ) }}
