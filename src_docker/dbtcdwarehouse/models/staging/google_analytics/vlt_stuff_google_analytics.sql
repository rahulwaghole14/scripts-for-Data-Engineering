{{
    config(
        tags=['hexa_google_analytics']
    )
}}

{%- set yaml_metadata -%}
depends_on: stg_hexa_google_analytics__events_fresh_yesterday_v2
source_model: stg_hexa_google_analytics__events_fresh_yesterday_v2
derived_columns:
  RECORD_SOURCE: "!GOOGLE_ANALYTICS.hexa.CO.NZ"
  LOAD_DATE: event_timestamp_nz
  EFFECTIVE_FROM: event_timestamp_nz
hashed_columns:
  GA_HASH: "event_timestamp_nz"
  GA_HASHDIFF:
    is_hashdiff: true
    columns:
      - event_timestamp_nz
      - event_date_nz
      - event_name
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
