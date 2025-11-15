{{ config(
    materialized='incremental',
    tags=['vault_adw']
) }}
{%- set yaml_metadata -%}
depends_on: "vlt_stg_showcaseturbo__adrevenue"
source_model: "vlt_stg_showcaseturbo__adrevenue"
src_pk: "AD_REPORT_HASH"
src_hashdiff:
  source_column: "AD_REPORT_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "AD_REPORT_KEY"
  - "AD_OUR_REF"
  - "AD_ADVERTISER_ID"
  - "AD_BILLING_CUSTOMER_ID"
  - "AD_ORDER_DATE"
  - "AD_START_DATE"
  - "AD_END_DATE"
  - "AD_BILLING_DATE"
  - "AD_X"
  - "AD_ADVERTISER_NAME"
  - "AD_ADVERTISER_CATEGORY"
  - "AD_PRODUCT_CAMPAIGN"
  - "AD_AGENCY_NAME"
  - "AD_PROPERTY"
  - "AD_LOCATION"
  - "AD_COMBINED"
  - "AD_AD_UNIT"
  - "AD_RATING_METHOD"
  - "AD_NUM_UNITS"
  - "AD_CLIENT_REF"
  - "AD_SALES_REP"
  - "AD_AGREED_PRICE"
  - "AD_OTHER_CHARGES"
  - "AD_OTHER_CHARGES_DESC"
  - "AD_GROSS_BILL_AMOUNT"
  - "AD_COMMISSION"
  - "AD_NET_BILL_AMOUNT"
  - "AD_LOCATION_CODE"
  - "AD_STAFF_CODE"
  - "AD_CAMPAIGN_REFERENCE"
  - "AD_FILENAME"
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
