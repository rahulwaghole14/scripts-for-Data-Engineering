{{
  config(
    materialized = 'table',
    tags = 'adw',
    partition_by = {
      "field": "DATE",
      "data_type": "date",
      "granularity": "day"
    },
    )
}}

-- update before 8am

with adrevenuedata as (

    select

        AD_REPORT_KEY
        , AD_REPORT_HASH
        , RECORD_SOURCE
        , CAMPAIGN_ID
        , LINE_ID
        , MONTH_UNIQUE_ID
		    , DATE
        , DATE_ENTERED
        , ORIGIN
        , SOURCE
        , PRIMARY_REP_GROUP
        , SOURCE_OR_GL
        , AGENCY_LEGACY
        , ADVERTISER_ID
      	, ADVERTISER_LEGACY
        , CUSTOMERNUMBER
        , ADVERTISER_NAME
        , PIB_CATEGORY_DESC
        , AGENCY_NAME
        , PRODUCT_NAME
        , PRODUCT_ID
        , SIZE_DESC
        , AD_TYPE_ID
        , FILEUPDATEDATETIME
      	, GL_TYPE_ID
      	, GL_TYPES_DESCRIPTION
        , PAID_ADVERTISING
		    , CURRENT_REP_NAME
		    , PLATFORM
        , PRODUCT_TYPE
        , PRIMARY_GROUP_ID
        , CLIENT_TYPE_ID
        , CAMPAIGN_TYPE
        , CAMPAIGN_STATUS_CODE
        , LINE_CANCEL_STATUS_ID
        , CURRENCY_EXCHANGE_RATE
        , CURRENCY_CODE
        , AGENCY_COMMISSION_PERCENT
        , NO_AGY_COMM_IND
        , CURRENT_REP_ID
        , CURRENT_REP_PCT
        , NET_REP_AMOUNT
        {# , MONTH_ACTUAL_AMT
        , MONTH_EST_AMT #}
        , PRODUCT_GROUPING
        , PRIMARY_REP_GROUP_ID
        , AGENCY_ID
        , AD_INTERNET_CAMPAIGNS_BRAND_ID
        , BRAND_PIB_CODE
        , SECTION
        , REVENUE
        , SPLIT(CURRENT_REP_NAME, ',') AS CURRENT_REP_NAME_ARRAY
        , SPLIT(CURRENT_REP_ID, ',') AS CURRENT_REP_ID_ARRAY
        , SPLIT(CURRENT_REP_PCT, ',') AS CURRENT_REP_PCT_ARRAY
        , SPLIT(REGEXP_REPLACE(NET_REP_AMOUNT, r'(\.\d{2}),', r'\1;'), ';') AS NET_REP_AMOUNT_ARRAY

    from {{ ref('int_ad_revenue__merge') }}

)

SELECT
  a.* EXCEPT (
      CURRENT_REP_NAME
    , CURRENT_REP_ID
    , CURRENT_REP_PCT
    , NET_REP_AMOUNT
    , CURRENT_REP_NAME_ARRAY
    , CURRENT_REP_ID_ARRAY
    , CURRENT_REP_PCT_ARRAY
    , NET_REP_AMOUNT_ARRAY)
  , IFNULL(CURRENT_REP_NAME_ARRAY[offset],'(not set)') AS CURRENT_REP_NAME
  , IFNULL(CURRENT_REP_ID_ARRAY[offset],'(not set)') AS CURRENT_REP_ID
  , SAFE_CAST(IFNULL(CURRENT_REP_PCT_ARRAY[offset], '0') AS FLOAT64) AS CURRENT_REP_PCT
  , ROUND(
    REVENUE * (SAFE_CAST(IFNULL(CURRENT_REP_PCT_ARRAY[offset], '0') AS FLOAT64) / 100)
    ,2
   ) AS REP_REVENUE
  , SAFE_CAST(IFNULL(NET_REP_AMOUNT_ARRAY[offset], '0') AS FLOAT64) AS NET_REP_AMOUNT
FROM adrevenuedata a
  CROSS JOIN UNNEST(
    IFNULL(a.CURRENT_REP_ID_ARRAY,ARRAY['(not set)'])
    ) WITH OFFSET
