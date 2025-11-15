{{ config(
    materialized='incremental',
    tags=['vault_adw']
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_stg_showcase_plus__adrevenue"
source_model: "vlt_stg_showcase_plus__adrevenue"
src_pk: "AD_REPORT_HASH"
src_hashdiff:
  source_column: "AD_REPORT_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "AD_REPORT_KEY"
  - "AD_CURRENT_REP_NAME"
  - "DATE"
  - "AD_ADVERTISER_NAME"
  - "AD_PRODUCT_ID"
  - "AD_REVENUE"
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
