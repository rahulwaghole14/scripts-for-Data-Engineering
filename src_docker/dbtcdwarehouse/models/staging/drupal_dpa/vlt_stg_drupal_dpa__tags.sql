{%- set yaml_metadata -%}
depends_on: stg_drupal_dpa__tags
source_model: stg_drupal_dpa__tags
derived_columns:
  RECORD_SOURCE: "!DRUPALDPA_TAG_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS
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
      - TAG
      - TAG_CLASS
      - RECORD_SOURCE
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
