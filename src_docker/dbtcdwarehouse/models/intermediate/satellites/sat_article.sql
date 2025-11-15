{{ config(
    materialized='incremental',
    tags=['vault_sentiment']
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_stg_composer__articles"
source_model: "vlt_stg_composer__articles"
src_pk: "ARTICLE_HASH"
src_hashdiff:
  source_column: "ARTICLE_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "ARTICLE_KEY"
  - "ARTICLE_TITLE"
  - "ARTICLE_PUBLISHED_DTS"
  - "ARTICLE_SOURCE"
  - "ARTICLE_BRAND"
  - "ARTICLE_CATEGORY"
  - "ARTICLE_CATEGORY_1"
  - "ARTICLE_CATEGORY_2"
  - "ARTICLE_CATEGORY_3"
  - "ARTICLE_CATEGORY_4"
  - "ARTICLE_CATEGORY_5"
  - "ARTICLE_CATEGORY_6"
  - "ARTICLE_PRINT_SLUG"
  - "ARTICLE_AUTHOR"
  - "ARTICLE_WORD_COUNT"
  - "ARTICLE_IMAGE_COUNT"
  - "ARTICLE_VIDEO_COUNT"
  - "ARTICLE_ADVERTISEMENT"
  - "ARTICLE_SPONSORED"
  - "ARTICLE_PROMOTED_FLAG"
  - "ARTICLE_COMMENTS_FLAG"
  - "ARTICLE_HOME_FLAG"
src_eff: "EFFECTIVE_FROM"
src_ldts: "LOAD_DATE"
src_source: "RECORD_SOURCE"
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ automate_dv.sat(src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
