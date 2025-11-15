{{ config(
    materialized = 'incremental',
    tags=['vault_adw'],
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

{%- set depends_on = [
    "vlt_stg_showcaseturbo__adrevenue",
    "vlt_stg_adbook__adrevenue",
    "vlt_stg_genera__adrevenue",
    "vlt_stg_naviga__adrevenue"
    ] -%}
{%- set source_model = [
    "vlt_stg_showcaseturbo__adrevenue",
    "vlt_stg_adbook__adrevenue",
    "vlt_stg_genera__adrevenue",
    "vlt_stg_naviga__adrevenue"
    ] -%}
{%- set src_pk = "AD_REPORT_HASH" -%}
{%- set src_nk = "AD_REPORT_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
src_source=src_source, source_model=source_model) }}
