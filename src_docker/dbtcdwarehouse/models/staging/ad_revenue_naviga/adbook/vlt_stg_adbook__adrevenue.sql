{{
  config(
    tags = ['adw']
    )
}}

{% set yaml_metadata %}
depends_on: stg_adbook__adrevenue
source_model: stg_adbook__adrevenue
derived_columns:
  RECORD_SOURCE: "!ADBOOK_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS
  EFFECTIVE_FROM: EFFECTIVE_FROM
hashed_columns:
  AD_REPORT_HASH:
      - AD_REPORT_KEY
      - RECORD_SOURCE
  AD_REPORT_HASHDIFF:
    is_hashdiff: true
    columns:
      - AD_CAMPAIGN_ID
      - AD_LINE_ID
      - AD_MONTH_UNIQUE_ID
      - AD_MONTH_START_DATE
      - AD_DATE_ENTERED
      - AD_ISSUE_DATE
      - AD_FILEUPDATEDATETIME
      - AD_PRINT_PUB_IND
      - AD_PRIMARY_GROUP_ID
      - AD_PRODUCT_ID
      - AD_PRODUCT_NAME
      - AD_CLIENT_TYPE_ID
      - AD_ADVERTISER_ID
      - AD_ADVERTISER_NAME
      - AD_CAMPAIGN_TYPE
      - AD_CAMPAIGN_STATUS_CODE
      - AD_LINE_CANCEL_STATUS_ID
      - AD_CURRENCY_EXCHANGE_RATE
      - AD_CURRENCY_CODE
      - AD_AGENCY_COMMISSION_PERCENT
      - AD_NO_AGY_COMM_IND
      - AD_ACTUAL_LINE_LOCAL_AMOUNT
      - AD_EST_LINE_LOCAL_AMOUNT
      - AD_GROSS_LINE_LOCAL_AMOUNT
      - AD_NET_LINE_LOCAL_AMOUNT
      - AD_GROSS_LINE_FOREIGN_AMOUNT
      - AD_NET_LINE_FOREIGN_AMOUNT
      - AD_CURRENT_REP_ID
      - AD_CURRENT_REP_PCT
      - AD_CURRENT_REP_NAME
      - AD_NET_REP_AMOUNT
      - AD_MONTH_ACTUAL_AMT
      - AD_MONTH_EST_AMT
      - AD_AD_TYPE_ID
      - AD_PRODUCT_GROUPING
      - AD_PRIMARY_REP_GROUP
      - AD_PRIMARY_REP_GROUP_ID
      - AD_AGENCY_ID
      - AD_AGENCY_NAME
      - AD_AD_INTERNET_CAMPAIGNS_BRAND_ID
      - AD_BRAND_PIB_CODE
      - AD_PIB_CATEGORY_DESC
      - AD_SIZE_DESC
      - AD_GL_TYPE_ID
      - AD_GL_TYPES_DESCRIPTION
      - AD_ADVERTISER_LEGACY
      - AD_AGENCY_LEGACY
      - AD_AD_INTERNET_SECTIONS_SECTION_DESCRIPTION
{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns
) }}
