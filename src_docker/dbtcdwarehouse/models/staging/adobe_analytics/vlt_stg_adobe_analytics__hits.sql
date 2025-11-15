{{
  config(
    tags = ['stage_adobe']
    )
}}

{%- set yaml_metadata -%}
depends_on: stg_adobe_analytics__hits
source_model: stg_adobe_analytics__hits
derived_columns:
  RECORD_SOURCE: "!ADOBE_ANALYTICS_HITS_MASTHEAD.hexa.CO.NZ"
  LOAD_DATE: TABLE_SUFFIX_TIME
  EFFECTIVE_FROM: TABLE_SUFFIX_TIME
hashed_columns:
  ADOBE_HASH: "ADOBE_KEY"
  {# ADOBE_HASH:
     - HITID_HIGH
     - HITID_LOW
     - VISIT_NUM
     - VISIT_PAGE_NUM #}
  ADOBE_HASHDIFF:
    is_hashdiff: true
    columns:
      - HITID_HIGH
      - HITID_LOW
      - VISIT_NUM
      - VISIT_PAGE_NUM
      - TABLE_SUFFIX_VALUE
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
