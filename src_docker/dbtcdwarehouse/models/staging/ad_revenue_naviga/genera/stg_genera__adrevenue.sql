{{
  config(
    tags = ['adw']
    )
}}

with source as (
      select * from {{ source('genera', 'genera__adrevenue') }}
),

renamed as (

    select
        CONCAT(
          {{ adapter.quote("campaign_id") }}, '|',
          {{ adapter.quote("line_id") }}
          ) AS AD_REPORT_KEY,
        {{ adapter.quote("campaign_id") }} AS AD_CAMPAIGN_ID,
        {{ adapter.quote("line_id") }} AS AD_LINE_ID,
        TIMESTAMP({{ adapter.quote("issue_date") }}) AS RECORD_LOAD_DTS,
        TIMESTAMP({{ adapter.quote("issue_date") }}) AS EFFECTIVE_FROM,
        {{ adapter.quote("issue_date") }} AS AD_ISSUE_DATE,
        {{ adapter.quote("month_start_date") }} AS AD_MONTH_START_DATE,
        {{ adapter.quote("date_entered") }} AS AD_DATE_ENTERED,
        {{ adapter.quote("fileupdatedatetime") }} AS AD_FILEUPDATEDATETIME,
        {{ adapter.quote("print_pub_ind") }} AS AD_PRINT_PUB_IND,
        {{ adapter.quote("primary_group_id") }} AS AD_PRIMARY_GROUP_ID,
        {{ adapter.quote("product_id") }} AS AD_PRODUCT_ID,
        {{ adapter.quote("product_name") }} AS AD_PRODUCT_NAME,
        {{ adapter.quote("client_type_id") }} AS AD_CLIENT_TYPE_ID,
        {{ adapter.quote("advertiser_id") }} AS AD_ADVERTISER_ID,
        {{ adapter.quote("advertiser_name") }} AS AD_ADVERTISER_NAME,
        {{ adapter.quote("month_unique_id") }} AS AD_MONTH_UNIQUE_ID,
        {{ adapter.quote("campaign_type") }} AS AD_CAMPAIGN_TYPE,
        {{ adapter.quote("campaign_status_code") }} AS AD_CAMPAIGN_STATUS_CODE,
        {{ adapter.quote("line_cancel_status_id") }} AS AD_LINE_CANCEL_STATUS_ID,
        {{ adapter.quote("currency_exchange_rate") }} AS AD_CURRENCY_EXCHANGE_RATE,
        {{ adapter.quote("currency_code") }} AS AD_CURRENCY_CODE,
        {{ adapter.quote("agency_commission_percent") }} AS AD_AGENCY_COMMISSION_PERCENT,
        {{ adapter.quote("no_agy_comm_ind") }} AS AD_NO_AGY_COMM_IND,
        {{ adapter.quote("actual_line_local_amount") }} AS AD_ACTUAL_LINE_LOCAL_AMOUNT,
        {{ adapter.quote("est_line_local_amount") }} AS AD_EST_LINE_LOCAL_AMOUNT,
        {{ adapter.quote("gross_line_local_amount") }} AS AD_GROSS_LINE_LOCAL_AMOUNT,
        {{ adapter.quote("net_line_local_amount") }} AS AD_NET_LINE_LOCAL_AMOUNT,
        {{ adapter.quote("gross_line_foreign_amount") }} AS AD_GROSS_LINE_FOREIGN_AMOUNT,
        {{ adapter.quote("net_line_foreign_amount") }} AS AD_NET_LINE_FOREIGN_AMOUNT,
        {{ adapter.quote("current_rep_id") }} AS AD_CURRENT_REP_ID,
        {{ adapter.quote("current_rep_pct") }} AS AD_CURRENT_REP_PCT,
        {{ adapter.quote("current_rep_name") }} AS AD_CURRENT_REP_NAME,
        {{ adapter.quote("net_rep_amount") }} AS AD_NET_REP_AMOUNT,
        {{ adapter.quote("month_actual_amt") }} AS AD_MONTH_ACTUAL_AMT,
        {{ adapter.quote("month_est_amt") }} AS AD_MONTH_EST_AMT,
        {{ adapter.quote("ad_type_id") }} AS AD_AD_TYPE_ID,
        {{ adapter.quote("product_grouping") }} AS AD_PRODUCT_GROUPING,
        {{ adapter.quote("primary_rep_group") }} AS AD_PRIMARY_REP_GROUP,
        {{ adapter.quote("primary_rep_group_id") }} AS AD_PRIMARY_REP_GROUP_ID,
        {{ adapter.quote("agency_id") }} AS AD_AGENCY_ID,
        {{ adapter.quote("agency_name") }} AS AD_AGENCY_NAME,
        {{ adapter.quote("ad_internet_campaigns_brand_id") }} AS AD_AD_INTERNET_CAMPAIGNS_BRAND_ID,
        {{ adapter.quote("brand_pib_code") }} AS AD_BRAND_PIB_CODE,
        {{ adapter.quote("pib_category_desc") }} AS AD_PIB_CATEGORY_DESC,
        {{ adapter.quote("size_desc") }} AS AD_SIZE_DESC,
        {{ adapter.quote("gl_type_id") }} AS AD_GL_TYPE_ID,
        {{ adapter.quote("gl_types_description") }} AS AD_GL_TYPES_DESCRIPTION,
        {{ adapter.quote("advertiser_legacy") }} AS AD_ADVERTISER_LEGACY,
        {{ adapter.quote("agency_legacy") }} AS AD_AGENCY_LEGACY,
        {{ adapter.quote("ad_internet_sections_section_description") }} AS AD_AD_INTERNET_SECTIONS_SECTION_DESCRIPTION

    from source

)

select * from renamed
