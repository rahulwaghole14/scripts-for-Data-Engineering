{{ config(
    materialized = 'incremental',
    tags=['vault_sentiment']
) }}
{%- set depends_on = "vlt_stg_qualtrics__responses" -%}
{%- set source_model = "vlt_stg_qualtrics__responses" -%}
{%- set src_pk = "RESPONSE_HASH" -%} -- change
{%- set src_nk = "RESPONSE_KEY" -%} -- change to response_key
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
src_source=src_source, source_model=source_model) }}
