{{ config(
    materialized='incremental',
    tags = ['vlt_user'],
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_account_management__user_profiles"
source_model: "vlt_account_management__user_profiles"
src_pk: "USER_HASH"
src_hashdiff:
  source_column: "USER_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "USER_KEY"
  - "SCHEMAS"
  - "EXTERNALID"
  - "USERNAME"
  - "DISPLAYNAME"
  - "META"
  - "NAME"
  - "ACTIVE"
  - "EMAILS"
  - "ADDRESSES"
  - "PHONENUMBERS"
  - "ROLES"
  - "USER_CUSTOM_EXTENSION"
  - "MARKETING_ID"
  - "MARKETING_ID_EMAIL"
  - "TIMEZONE"
  - "PPID"
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
