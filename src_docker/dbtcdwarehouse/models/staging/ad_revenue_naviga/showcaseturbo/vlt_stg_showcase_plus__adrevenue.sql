{{
  config(
    tags = ['adw']
    )
}}

{% set yaml_metadata %}
depends_on: stg_showcase_plus_adrevenue
source_model: stg_showcase_plus_adrevenue
derived_columns:
  RECORD_SOURCE: "!SHOWCASEPLUS_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS
  EFFECTIVE_FROM: DATE
hashed_columns:
  AD_REPORT_HASH:
      - AD_REPORT_KEY
      - RECORD_SOURCE
  AD_REPORT_HASHDIFF:
    is_hashdiff: true
    columns:
      - AD_CURRENT_REP_NAME
      - DATE
      - AD_ADVERTISER_NAME
      - AD_PRODUCT_ID
      - AD_REVENUE

{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
) }}
