{{ config(
    materialized='incremental',
    tags=['vault_sentiment']
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_stg_drupal_dpa__articles"
source_model: "vlt_stg_drupal_dpa__articles"
src_pk: "ARTICLE_HASH"
src_hashdiff:
  source_column: "ARTICLE_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "ARTICLE_KEY"
  - "ARTICLE_ID"
  - "ARTICLE_DRUPALID"
  - "ARTICLE_CONTENTTYPE"
  - "ARTICLE_URL"
  - "ARTICLE_HEADLINE"
  - "ARTICLE_HEADLINEPRIMARY"
  - "ARTICLE_SLUG"
  - "ARTICLE_BYLINE"
  - "ARTICLE_STATUS"
  - "ARTICLE_CREATEDDATE"
  - "ARTICLE_PUBLISHEDDATE"
  - "ARTICLE_UPDATEDDATE"
  - "ARTICLE_TEASER_SHORTHEADLINE"
  - "ARTICLE_TEASER_INTRO"
  - "ARTICLE_BODY_AGG"
  - "ARTICLE_SOURCE"
  - "ARTICLE_SECTION"
  - "ARTICLE_TEAMS_AGG"
  - "ARTICLE_TYPEOFWORKLABEL_AGG"
  - "ARTICLE_LIVEBLOG"
  - "ARTICLE_SEARCHINDEXED"
  - "ARTICLE_TOPICS_AGG"
  - "ARTICLE_ENTITIES_AGG"
  - "ARTICLE_SENSITIVITY"
  - "ARTICLE_SENTIMENT"
  - "ARTICLE_USERNEED"
  - "ARTICLE_NEWSVALUE"
  - "ARTICLE_LIFETIME"
  - "ARTICLE_COMMENTS"
  - "ARTICLE_MAINPUBLICATIONCHANNEL_NAME"
  - "ARTICLE_MAINPUBLICATIONCHANNEL_KEY"
  - "ARTICLE_AUTHOR_ID"
  - "ARTICLE_AUTHOR_NAME"
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
