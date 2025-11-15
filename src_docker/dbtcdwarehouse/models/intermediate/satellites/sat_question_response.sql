{{ config(
    materialized='incremental',
    tags=['vault_sentiment']
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_stg_qualtrics__responses"
source_model: "vlt_stg_qualtrics__responses"
src_pk: "RESPONSE_HASH"
src_hashdiff:
  source_column: "RESPONSE_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
      - "RESPONSE_RESPONSEID"
      - "RESPONSE_QUESTIONID"
      - "RESPONSE_VALUE"
      - "RESPONSE_SURVEYID"
      - "RESPONSE_SURVEYTYPE"
      - "QUESTION_SURVEYQUESTIONID"
      - "RESPONSE_STARTDATE"
      - "RESPONSE_ENDDATE"
      - "RESPONSE_STATUS"
      - "RESPONSE_IPADDRESS"
      - "RESPONSE_DURATION"
      - "RESPONSE_RECORDEDDATE"
      - "RESPONSE_LOCATIONLATITUDE"
      - "RESPONSE_LOCATIONLONGITUDE"
      - "RESPONSE_REGION_NAME"
      - "RESPONSE_DISTRIBUTIONCHANNEL"
      - "RESPONSE_PAGETYPE"
      - "RESPONSE_SYSENV"
      - "RESPONSE_PAGEREFERRER"
      - "RESPONSE_SITEREFERRER"
      - "RESPONSE_CURRENTPAGEURL"
      - "QUESTION_SURVEYID"
      - "QUESTION_QUESTIONID"
      - "QUESTION_QUESTIONTEXT"
      - "QUESTION_DEFAULTCHOICES"
      - "QUESTION_DATAEXPORTTAG"
      - "QUESTION_QUESTIONTYPE"
      - "QUESTION_SELECTOR"
      - "QUESTION_DESCRIPTION"
      - "QUESTION_SUBSELECTOR"
      - "QUESTION_CHOICEORDER"
      - "ARTICLE_KEY"
      - "RESPONSE_hexa_ACCOUNT_ID"
src_eff: "EFFECTIVE_FROM"
src_ldts: "LOAD_DATE"
src_source: "RECORD_SOURCE"
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %} --noqa
{{ automate_dv.sat(
                   src_pk=metadata_dict["src_pk"],
                   src_hashdiff=metadata_dict["src_hashdiff"],
                   src_payload=metadata_dict["src_payload"],
                   src_eff=metadata_dict["src_eff"],
                   src_ldts=metadata_dict["src_ldts"],
                   src_source=metadata_dict["src_source"],
                   source_model=metadata_dict["source_model"])   }}
