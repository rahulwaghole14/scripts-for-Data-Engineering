{{ config(
    materialized = 'incremental',
    tags=['vlt_user']
) }}
{%- set depends_on = ["vlt_account_management__user_profiles"] -%}
{%- set source_model = ["vlt_account_management__user_profiles"] -%}
{%- set src_pk = "USER_HASH" -%}
{%- set src_nk = "USER_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
src_source=src_source, source_model=source_model) }}
