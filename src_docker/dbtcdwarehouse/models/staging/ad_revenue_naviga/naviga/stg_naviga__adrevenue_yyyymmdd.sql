{{
  config(
    tags = ['adw']
    )
}}

{% set latest_table = get_latest_table() %}

with source as (
      select * from {{ source('naviga', 'naviga__adrevenue_bq_*') }}
      -- grab the data from the latest dataset only
      WHERE _TABLE_SUFFIX = '{{latest_table}}'

),
renamed as (
    select
      CONCAT(
      COALESCE(CAST({{ adapter.quote("Campaign_ID") }} AS STRING), ''), '|',
      COALESCE(CAST({{ adapter.quote("Line_ID") }} AS STRING), ''), '|',
      COALESCE(CAST({{ adapter.quote("Month_Unique_ID") }} AS STRING), ''), '|',
      COALESCE(CAST({{ adapter.quote("month_start_date") }} AS STRING), '')
      ) AS AD_REPORT_KEY,
      CAST(SPLIT(REPLACE({{ adapter.quote("Campaign_ID") }}, ',', ''), '.')[OFFSET(0)] AS INT64) AS AD_CAMPAIGN_ID,
     --CAST({{ adapter.quote("Campaign_ID") }} AS INT64) AS AD_CAMPAIGN_ID,
      {{ adapter.quote("Line_ID") }} AS AD_LINE_ID,
      SAFE_CAST(
        REGEXP_REPLACE({{ adapter.quote("Month_Start_Date") }}, r'^(\d{1,2})/(\d{1,2})/(\d{4})$', r'\3-\1-\2')
        AS TIMESTAMP
      ) AS AD_MONTH_START_DATE,
      --TIMESTAMP_MICROS(CAST({{ adapter.quote("Month_Start_Date") }} / 1000 AS INT64)) AS AD_MONTH_START_DATE,
      SAFE_CAST({{ adapter.quote("Month_Unique_ID") }} AS FLOAT64) AS AD_MONTH_UNIQUE_ID,
      {{ adapter.quote("Current_Rep_ID") }} AS AD_CURRENT_REP_ID,
      {{ adapter.quote("Current_Rep_Pct") }} AS AD_CURRENT_REP_PCT,
      SAFE_CAST(
      REGEXP_REPLACE({{ adapter.quote("Issue_Date") }}, r'^(\d{1,2})/(\d{1,2})/(\d{4})$', r'\3-\1-\2')
      AS TIMESTAMP
      ) AS AD_ISSUE_DATE,
      --TIMESTAMP_MICROS(CAST({{ adapter.quote("Issue_Date") }} / 1000 AS INT64)) AS AD_ISSUE_DATE,
      {{ adapter.quote("Print_Pub_Ind") }} AS AD_PRINT_PUB_IND,
      {{ adapter.quote("Primary_Group_ID") }} AS AD_PRIMARY_GROUP_ID,
      {{ adapter.quote("Product_ID") }} AS AD_PRODUCT_ID,
      {{ adapter.quote("Product_Name") }} AS AD_PRODUCT_NAME,
      {{ adapter.quote("Client_Type_ID") }} AS AD_CLIENT_TYPE_ID,
      {{ adapter.quote("Advertiser_Id") }} AS AD_ADVERTISER_ID,
      {{ adapter.quote("Advertiser_Name") }} AS AD_ADVERTISER_NAME,
      {{ adapter.quote("Campaign_Type") }} AS AD_CAMPAIGN_TYPE,
      {{ adapter.quote("Campaign_Status_Code") }} AS AD_CAMPAIGN_STATUS_CODE,
      CAST({{ adapter.quote("Line_Cancel_Status_ID") }} AS STRING) AS AD_LINE_CANCEL_STATUS_ID,
      SAFE_CAST({{ adapter.quote("Currency_Exchange_Rate") }} AS INT64) AS AD_CURRENCY_EXCHANGE_RATE,
      SAFE_CAST({{ adapter.quote("Currency_Code") }} AS INT64) AS AD_CURRENCY_CODE,
      {{ adapter.quote("Agency_Commission_Percent") }} AS AD_AGENCY_COMMISSION_PERCENT,
      {{ adapter.quote("No_Agy_Comm_Ind") }} AS AD_NO_AGY_COMM_IND,
      {{ adapter.quote("Actual_Line_Local_Amount") }} AS AD_ACTUAL_LINE_LOCAL_AMOUNT,
      {{ adapter.quote("Est_Line_Local_Amount") }} AS AD_EST_LINE_LOCAL_AMOUNT,
      {{ adapter.quote("Gross_Line_Local_Amount") }} AS AD_GROSS_LINE_LOCAL_AMOUNT,
      {{ adapter.quote("Net_Line_Local_Amount") }} AS AD_NET_LINE_LOCAL_AMOUNT,
      {{ adapter.quote("Gross_Line_Foreign_Amount") }} AS AD_GROSS_LINE_FOREIGN_AMOUNT,
      {{ adapter.quote("Net_Line_Foreign_Amount") }} AS AD_NET_LINE_FOREIGN_AMOUNT,
      {{ adapter.quote("Current_Rep_Name") }} AS AD_CURRENT_REP_NAME,
      {{ adapter.quote("Net_Rep_Amount") }} AS AD_NET_REP_AMOUNT,
      {{ adapter.quote("Month_Actual_Amt") }} AS AD_MONTH_ACTUAL_AMT,
      {{ adapter.quote("Month_Est_Amt") }} AS AD_MONTH_EST_AMT,
      SAFE_CAST(
      REGEXP_REPLACE({{ adapter.quote("Date_Entered") }} , r'^(\d{1,2})/(\d{1,2})/(\d{4})$', r'\3-\1-\2')
      AS TIMESTAMP
      ) AS AD_DATE_ENTERED,
      --TIMESTAMP_MICROS(CAST({{ adapter.quote("Date_Entered") }} / 1000 AS INT64)) AS AD_DATE_ENTERED,
      {{ adapter.quote("Ad_Type_ID") }} AS AD_AD_TYPE_ID,
      {{ adapter.quote("Product_Grouping") }} AS AD_PRODUCT_GROUPING,
      {{ adapter.quote("Primary_Rep_Group") }} AS AD_PRIMARY_REP_GROUP,
      {{ adapter.quote("Primary_Rep_Group_ID") }} AS AD_PRIMARY_REP_GROUP_ID,
      {{ adapter.quote("Agency_ID") }} AS AD_AGENCY_ID,
      {{ adapter.quote("Agency_Name") }} AS AD_AGENCY_NAME,
      {{ adapter.quote("AD_Internet_Campaigns_Brand_Id") }} AS AD_AD_INTERNET_CAMPAIGNS_BRAND_ID,
      {{ adapter.quote("Brand_PIB_Code") }} AS AD_BRAND_PIB_CODE,
      {{ adapter.quote("PIB_Category_Desc") }} AS AD_PIB_CATEGORY_DESC,
      {{ adapter.quote("Size_Desc") }} AS AD_SIZE_DESC,
      {{ adapter.quote("GL_Type_ID") }} AS AD_GL_TYPE_ID,
      {{ adapter.quote("GL_Types_Description") }} AS AD_GL_TYPES_DESCRIPTION,
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', {{ adapter.quote("Timestamp") }}) AS  RECORD_LOAD_DTS,
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', {{ adapter.quote("Timestamp") }}) AS  EFFECTIVE_FROM,
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', {{ adapter.quote("Timestamp") }}) AS  AD_FILEUPDATEDATETIME,
      {# TIMESTAMP_MICROS(CAST({{ adapter.quote("Timestamp") }} / 1000 AS INT64)) AS RECORD_LOAD_DTS,
      TIMESTAMP_MICROS(CAST({{ adapter.quote("Timestamp") }} / 1000 AS INT64)) AS EFFECTIVE_FROM,
      TIMESTAMP_MICROS(CAST({{ adapter.quote("Timestamp") }} / 1000 AS INT64))  AS AD_FILEUPDATEDATETIME, #}
      {{ adapter.quote("Advertiser_Legacy") }} AS AD_ADVERTISER_LEGACY,
      {{ adapter.quote("Agency_Legacy") }} AS AD_AGENCY_LEGACY,
      {{ adapter.quote("AD_Internet_Sections_Section_Description") }} AS AD_AD_INTERNET_SECTIONS_SECTION_DESCRIPTION,
      {{ adapter.quote("Orig_Rep_Report_Ids") }} AS AD_ORIG_REP_REPORT_IDS,
      {{ adapter.quote("Curr_Rep_Report_Ids") }} AS AD_CURR_REP_REPORT_IDS,
      {{ adapter.quote("Orig_Rep_ID") }} AS AD_ORIG_REP_ID,
      {{ adapter.quote("Est_Qty") }} AS AD_EST_QTY,
      {{ adapter.quote("Month_Actual_Imps") }} AS AD_MONTH_ACTUAL_IMPS

    from source
)
select * from renamed
