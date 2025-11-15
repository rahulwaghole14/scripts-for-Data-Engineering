{{ config(
    materialized = 'incremental',
    tags=['vault_adobe'],
    partition_by={
      "field": "LOAD_DATE",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}
{%- set depends_on = ["vlt_stg_adobe_analytics__hits",
                        "vlt_stg_adobe_analytics__hits_replatform"]   -%}
{%- set source_model = ["vlt_stg_adobe_analytics__hits",
                        "vlt_stg_adobe_analytics__hits_replatform"]   -%}
{%- set src_pk = "ADOBE_HASH" -%}
{%- set src_nk = "ADOBE_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
src_source=src_source, source_model=source_model) }}
