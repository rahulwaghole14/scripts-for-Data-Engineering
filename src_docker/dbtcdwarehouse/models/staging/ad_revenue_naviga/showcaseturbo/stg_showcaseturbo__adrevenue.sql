{{
  config(
    tags = ['adw']
    )
}}

with source as (
      select * from {{ source('showcaseturbo', 'showcaseturbo__adrevenue') }}
),
renamed as (
    select
        {{ adapter.quote("our_ref") }} AS AD_REPORT_KEY,
        TIMESTAMP({{ adapter.quote("order_date") }}) AS RECORD_LOAD_DTS,
        TIMESTAMP({{ adapter.quote("order_date") }}) AS EFFECTIVE_FROM,
        {{ adapter.quote("our_ref") }} AS AD_OUR_REF,
        {{ adapter.quote("advertiser_id") }} AS AD_ADVERTISER_ID,
        {{ adapter.quote("billing_customer_id") }} AS AD_BILLING_CUSTOMER_ID,
        {{ adapter.quote("order_date") }} AS AD_ORDER_DATE,
        {{ adapter.quote("start_date") }} AS AD_START_DATE,
        {{ adapter.quote("end_date") }} AS AD_END_DATE,
        {{ adapter.quote("billing_date") }} AS AD_BILLING_DATE,
        {{ adapter.quote("x") }} AS AD_X,
        {{ adapter.quote("advertiser_name") }} AS AD_ADVERTISER_NAME,
        {{ adapter.quote("advertiser_category") }} AS AD_ADVERTISER_CATEGORY,
        {{ adapter.quote("product_campaign") }} AS AD_PRODUCT_CAMPAIGN,
        {{ adapter.quote("agency_name") }} AS AD_AGENCY_NAME,
        {{ adapter.quote("property") }} AS AD_PROPERTY,
        {{ adapter.quote("location") }} AS AD_LOCATION,
        {{ adapter.quote("combined") }} AS AD_COMBINED,
        {{ adapter.quote("ad_unit") }} AS AD_AD_UNIT,
        {{ adapter.quote("rating_method") }} AS AD_RATING_METHOD,
        {{ adapter.quote("num_units") }} AS AD_NUM_UNITS,
        {{ adapter.quote("client_ref") }} AS AD_CLIENT_REF,
        {{ adapter.quote("sales_rep") }} AS AD_SALES_REP,
        {{ adapter.quote("agreed_price") }} AS AD_AGREED_PRICE,
        {{ adapter.quote("other_charges") }} AS AD_OTHER_CHARGES,
        {{ adapter.quote("other_charges_desc") }} AS AD_OTHER_CHARGES_DESC,
        {{ adapter.quote("gross_bill_amount") }} AS AD_GROSS_BILL_AMOUNT,
        {{ adapter.quote("commission") }} AS AD_COMMISSION,
        {{ adapter.quote("net_bill_amount") }} AS AD_NET_BILL_AMOUNT,
        {{ adapter.quote("location_code") }} AS AD_LOCATION_CODE,
        {{ adapter.quote("staff_code") }} AS AD_STAFF_CODE,
        {{ adapter.quote("campaign_reference") }} AS AD_CAMPAIGN_REFERENCE,
        {{ adapter.quote("filename") }} AS AD_FILENAME

    from source
)
select * from renamed
