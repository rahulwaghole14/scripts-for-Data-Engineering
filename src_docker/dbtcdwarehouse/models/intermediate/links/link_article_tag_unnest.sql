{{ config(
    materialized='incremental',
    tags=['vault_sentiment']
) }}
{%- set depends_on = ["vlt_stg_composer__tags_unnest",
"vlt_stg_parsely__article_tags_unnest",
"vlt_stg_drupal_dpa__tags"] -%}
{%- set source_model = ["vlt_stg_composer__tags_unnest",
"vlt_stg_parsely__article_tags_unnest",
"vlt_stg_drupal_dpa__tags"] -%}
{%- set src_pk = "TAG_ARTICLE_HASH"-%}
{%- set src_fk = ["TAG_HASH", "ARTICLE_HASH"] -%}
{%- set src_ldts = "LOAD_DATE"  -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}
