
{%- set yaml_metadata -%}
depends_on: stg_qualtrics_api__responses
source_model: stg_qualtrics_api__responses
derived_columns:
  RECORD_SOURCE: "!QUALTRICS_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS
  EFFECTIVE_FROM: EFFECTIVE_FROM
hashed_columns:
  RESPONSE_HASH:
      - RESPONSE_SURVEYID
      - RESPONSE_RESPONSEID
      - RESPONSE_QUESTIONID
  RESPONSE_HASHDIFF:
    is_hashdiff: true
    columns:
      - RESPONSE_RESPONSEID
      - RESPONSE_QUESTIONID
      - RESPONSE_VALUE
      - RESPONSE_SURVEYID
      - RESPONSE_SURVEYTYPE
      - QUESTION_SURVEYQUESTIONID
      - RESPONSE_STARTDATE
      - RESPONSE_ENDDATE
      - RESPONSE_STATUS
      - RESPONSE_IPADDRESS
      - RESPONSE_DURATION
      - RESPONSE_RECORDEDDATE
      - RESPONSE_LOCATIONLATITUDE
      - RESPONSE_LOCATIONLONGITUDE
      - RESPONSE_REGION_NAME
      - RESPONSE_DISTRIBUTIONCHANNEL
      - RESPONSE_PAGETYPE
      - RESPONSE_SYSENV
      - RESPONSE_PAGEREFERRER
      - RESPONSE_SITEREFERRER
      - RESPONSE_CURRENTPAGEURL
      - QUESTION_SURVEYID
      - QUESTION_QUESTIONID
      - QUESTION_QUESTIONTEXT
      - QUESTION_DEFAULTCHOICES
      - QUESTION_DATAEXPORTTAG
      - QUESTION_QUESTIONTYPE
      - QUESTION_SELECTOR
      - QUESTION_DESCRIPTION
      - QUESTION_SUBSELECTOR
      - QUESTION_CHOICEORDER
      - ARTICLE_KEY
      - RESPONSE_hexa_ACCOUNT_ID
  ARTICLE_HASH: "ARTICLE_KEY"
  RESPONSE_ARTICLE_HASH:
      - RESPONSE_KEY
      - ARTICLE_KEY
{%- endset -%}
{%- set metadata_dict = fromyaml(yaml_metadata) -%} -- noqa
{%- set source_model = metadata_dict['source_model'] -%}
{%- set derived_columns = metadata_dict['derived_columns'] -%}
{%- set hashed_columns = metadata_dict['hashed_columns'] -%}
{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
) }} -- noqa
