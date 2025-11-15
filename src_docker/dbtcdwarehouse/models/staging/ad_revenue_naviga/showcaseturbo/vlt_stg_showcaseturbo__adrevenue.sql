{{
  config(
    tags = ['adw']
    )
}}

{% set yaml_metadata %}
depends_on: stg_showcaseturbo__adrevenue
source_model: stg_showcaseturbo__adrevenue
derived_columns:
  RECORD_SOURCE: "!SHOWCASETURBO_hexa.CO.NZ"
  LOAD_DATE: RECORD_LOAD_DTS
  EFFECTIVE_FROM: EFFECTIVE_FROM
hashed_columns:
  AD_REPORT_HASH:
      - AD_REPORT_KEY
      - RECORD_SOURCE
  AD_REPORT_HASHDIFF:
    is_hashdiff: true
    columns:
      - AD_OUR_REF
      - AD_ADVERTISER_ID
      - AD_BILLING_CUSTOMER_ID
      - AD_ORDER_DATE
      - AD_START_DATE
      - AD_END_DATE
      - AD_BILLING_DATE
      - AD_X
      - AD_ADVERTISER_NAME
      - AD_ADVERTISER_CATEGORY
      - AD_PRODUCT_CAMPAIGN
      - AD_AGENCY_NAME
      - AD_PROPERTY
      - AD_LOCATION
      - AD_COMBINED
      - AD_AD_UNIT
      - AD_RATING_METHOD
      - AD_NUM_UNITS
      - AD_CLIENT_REF
      - AD_SALES_REP
      - AD_AGREED_PRICE
      - AD_OTHER_CHARGES
      - AD_OTHER_CHARGES_DESC
      - AD_GROSS_BILL_AMOUNT
      - AD_COMMISSION
      - AD_NET_BILL_AMOUNT
      - AD_LOCATION_CODE
      - AD_STAFF_CODE
      - AD_CAMPAIGN_REFERENCE
      - AD_FILENAME
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
