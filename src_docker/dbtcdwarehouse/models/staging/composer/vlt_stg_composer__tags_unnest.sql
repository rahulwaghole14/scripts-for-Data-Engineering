{%- set yaml_metadata -%}
depends_on: stg_composer__tags_unnest
source_model: stg_composer__tags_unnest
derived_columns:
  RECORD_SOURCE: "!COMPOSER_TAG_hexa.CO.NZ"
  LOAD_DATE: LOAD_DATE
  EFFECTIVE_FROM: RECORD_LOAD_DTS
hashed_columns:
  TAG_HASH: "TAG_KEY"
  ARTICLE_HASH: "ARTICLE_KEY"
  TAG_ARTICLE_HASH:
     - TAG_KEY
     - ARTICLE_KEY
  TAG_HASHDIFF:
    is_hashdiff: true
    columns:
      - TAG_KEY
      - TAG
      - TAG_CLASS
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
