{{ config(
    materialized = 'incremental',
    tags=['vault_sentiment']
) }}
{%- set depends_on = ["vlt_stg_composer__articles","vlt_stg_drupal_dpa__articles"] -%}
{%- set source_model = ["vlt_stg_composer__articles","vlt_stg_drupal_dpa__articles"] -%}
{%- set src_pk = "ARTICLE_HASH" -%}
{%- set src_nk = "ARTICLE_KEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
src_source=src_source, source_model=source_model) }}
