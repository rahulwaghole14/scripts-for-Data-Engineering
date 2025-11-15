{{
  config(
    tags = ['vlt_stage_user']
    )
}}

{%- set yaml_metadata -%}
depends_on: stg_account_management__user_profiles
source_model: stg_account_management__user_profiles
derived_columns:
  RECORD_SOURCE: "!ACCOUNTMANAGEMENT.hexa.CO.NZ"
  LOAD_DATE: LOAD_DTS
  EFFECTIVE_FROM: LOAD_DTS
hashed_columns:
  USER_HASH: "USER_KEY"
  USER_HASHDIFF:
    is_hashdiff: true
    columns:
      - USER_KEY
      - SCHEMAS
      - EXTERNALID
      - USERNAME
      - DISPLAYNAME
      - META
      - NAME
      - ACTIVE
      - EMAILS
      - ADDRESSES
      - PHONENUMBERS
      - ROLES
      - USER_CUSTOM_EXTENSION
      - MARKETING_ID
      - MARKETING_ID_EMAIL
      - TIMEZONE
      - PPID
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
