{%- set yaml_metadata -%}
depends_on: stg_drupal_dpa__articles
source_model: stg_drupal_dpa__articles
derived_columns:
  RECORD_SOURCE: "!DRUPAL_DPA_hexa_MASTHEADS"
  LOAD_DATE: EFFECTIVE_FROM
  EFFECTIVE_FROM: EFFECTIVE_FROM
hashed_columns:
  ARTICLE_HASH: "ARTICLE_KEY"
  ARTICLE_HASHDIFF:
    is_hashdiff: true
    columns:
      - ARTICLE_KEY
      - ARTICLE_ID
      - ARTICLE_DRUPALID
      - ARTICLE_CONTENTTYPE
      - ARTICLE_URL
      - ARTICLE_HEADLINE
      - ARTICLE_HEADLINEPRIMARY
      - ARTICLE_SLUG
      - ARTICLE_BYLINE
      - ARTICLE_STATUS
      - ARTICLE_CREATEDDATE
      - ARTICLE_PUBLISHEDDATE
      - ARTICLE_UPDATEDDATE
      - ARTICLE_TEASER_SHORTHEADLINE
      - ARTICLE_TEASER_INTRO
      - ARTICLE_BODY_AGG
      - ARTICLE_SOURCE
      - ARTICLE_SECTION
      - ARTICLE_TEAMS_AGG
      - ARTICLE_TYPEOFWORKLABEL_AGG
      - ARTICLE_LIVEBLOG
      - ARTICLE_SEARCHINDEXED
      - ARTICLE_TOPICS_AGG
      - ARTICLE_ENTITIES_AGG
      - ARTICLE_SENSITIVITY
      - ARTICLE_SENTIMENT
      - ARTICLE_USERNEED
      - ARTICLE_NEWSVALUE
      - ARTICLE_LIFETIME
      - ARTICLE_COMMENTS
      - ARTICLE_MAINPUBLICATIONCHANNEL_NAME
      - ARTICLE_MAINPUBLICATIONCHANNEL_KEY
      - ARTICLE_AUTHOR_ID
      - ARTICLE_AUTHOR_NAME
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
