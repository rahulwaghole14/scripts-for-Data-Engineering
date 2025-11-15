{{
  config(
        tags=['adw']
    )
}}

WITH satdatanaviga AS (

    SELECT
        AD_ADVERTISER_LEGACY
        , AD_PIB_CATEGORY_DESC
    FROM {{ ref('sat_ad_revenue_naviga') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY AD_REPORT_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

, category_backfill AS (

    SELECT * FROM satdatanaviga
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(AD_ADVERTISER_LEGACY AS STRING)) = 1

)

, satdata AS (

    SELECT

        AD_REPORT_KEY
        , AD_REPORT_HASH
        , RECORD_SOURCE
        , CAST(AD_OUR_REF AS STRING) AS CAMPAIGN_ID
        , CAST(NULL AS STRING) AS LINE_ID
        , CAST(NULL AS STRING) AS MONTH_UNIQUE_ID
        , CAST(AD_ORDER_DATE AS DATE) AS DATE
        , CAST(AD_ORDER_DATE AS DATE) AS DATE_ENTERED
        , 'Digital' AS ORIGIN
        {# , 'Digital' as CHANNEL #}
        , 'Showcaseturbo' AS SOURCE
        , '(not set)' AS PRIMARY_REP_GROUP
        {# , '(not set)' as SALESREPTEAM #}
        , CASE
            WHEN AD_PROPERTY LIKE '%Showcase%' THEN 'Showcase Plus'
            WHEN AD_PROPERTY LIKE '%TURBO%' THEN 'TurboSEO'
            ELSE 'Unknown'
        END AS SOURCE_OR_GL

        , CASE
            WHEN AD_BILLING_CUSTOMER_ID = AD_ADVERTISER_ID THEN CAST(NULL AS STRING)
            ELSE CAST(AD_BILLING_CUSTOMER_ID AS STRING)
        END AS AGENCY_LEGACY

        , CASE
            WHEN AD_BILLING_CUSTOMER_ID = AD_ADVERTISER_ID THEN CAST(AD_BILLING_CUSTOMER_ID AS STRING)
            ELSE CAST(AD_ADVERTISER_ID AS STRING)
        END AS ADVERTISER_ID

        , CASE
            WHEN AD_BILLING_CUSTOMER_ID = AD_ADVERTISER_ID THEN CAST(AD_BILLING_CUSTOMER_ID AS STRING)
            ELSE CAST(AD_ADVERTISER_ID AS STRING)
        END AS ADVERTISER_LEGACY

        , CAST(NULL AS STRING) AS CUSTOMERNUMBER

        , AD_ADVERTISER_NAME AS ADVERTISER_NAME
        {# , AD_ADVERTISER_NAME AS CUSTOMERNAME #}
        , COALESCE(c.AD_PIB_CATEGORY_DESC, 'Other Services') AS PIB_CATEGORY_DESC
        , AD_AGENCY_NAME AS AGENCY_NAME
        , AD_PROPERTY AS PRODUCT_NAME
        , CAST(AD_LOCATION AS STRING) AS PRODUCT_ID
        , AD_AD_UNIT AS SIZE_DESC
        {# , AD_AD_UNIT AS SIZE #}
        , AD_RATING_METHOD AS AD_TYPE_ID
        , AD_GROSS_BILL_AMOUNT AS GROSS_LINE_LOCAL_AMOUNT
        , AD_NET_BILL_AMOUNT AS REVENUE
        , CAST(AD_FILENAME AS STRING) AS FILEUPDATEDATETIME

        , CASE
            WHEN AD_PROPERTY LIKE '%Showcase%' THEN 'NBLY'
            WHEN AD_PROPERTY LIKE '%TURBO%' THEN 'TURBOSEO'
            ELSE 'Unknown'
        END AS GL_TYPE_ID

        , CASE
            WHEN AD_PROPERTY LIKE '%Showcase%' THEN 'Neighbourly'
            WHEN AD_PROPERTY LIKE '%TURBO%' THEN 'TurboSEO'
            ELSE 'Unknown'
        END AS GL_TYPES_DESCRIPTION

		{# , CASE WHEN AD_PROPERTY like '%Showcase%' then 'Neighbourly'
		WHEN AD_PROPERTY like '%TURBO%' then 'TurboSEO'
		ELSE 'Unknown' END as TYPE #}

        , 'Paid Advertising (excl House and Contra)' AS PAID_ADVERTISING

        , '(not set)' AS CURRENT_REP_NAME
        {# , '(not set)' AS SALESREP #}

        , CASE
            WHEN AD_PROPERTY LIKE '%TURBO%' THEN 'TurboSEO'
            ELSE 'Showcase Plus'
        END AS PLATFORM

        , CASE
            WHEN AD_PROPERTY LIKE '%TURBO%' THEN 'Neighbourly'
            ELSE 'Showcase Plus'
        END AS PRODUCT_TYPE

        , CAST(NULL AS STRING) AS PRIMARY_GROUP_ID
        , CAST(NULL AS STRING) AS CLIENT_TYPE_ID
        , CAST(NULL AS STRING) AS CAMPAIGN_TYPE
        , CAST(NULL AS STRING) AS CAMPAIGN_STATUS_CODE
        , CAST(NULL AS STRING) AS LINE_CANCEL_STATUS_ID
        , CAST(NULL AS STRING) AS CURRENCY_EXCHANGE_RATE
        , CAST(NULL AS STRING) AS CURRENCY_CODE
        , NULL AS AGENCY_COMMISSION_PERCENT
        , CAST(NULL AS STRING) AS NO_AGY_COMM_IND
        , NULL AS ACTUAL_LINE_LOCAL_AMOUNT
        , NULL AS EST_LINE_LOCAL_AMOUNT
        , NULL AS GROSS_LINE_FOREIGN_AMOUNT
        , NULL AS NET_LINE_FOREIGN_AMOUNT
        , CAST(NULL AS STRING) AS CURRENT_REP_ID
        , CAST(NULL AS STRING) AS CURRENT_REP_PCT
        , CAST(NULL AS STRING) AS NET_REP_AMOUNT
        , CAST(NULL AS STRING) AS MONTH_ACTUAL_AMT
        , CAST(NULL AS STRING) AS MONTH_EST_AMT
        , CAST(NULL AS STRING) AS PRODUCT_GROUPING
        , CAST(NULL AS STRING) AS PRIMARY_REP_GROUP_ID
        , CAST(NULL AS STRING) AS AGENCY_ID
        , CAST(NULL AS STRING) AS AD_INTERNET_CAMPAIGNS_BRAND_ID
        , CAST(NULL AS STRING) AS BRAND_PIB_CODE
        , CAST(NULL AS STRING) AS SECTION
        , CAST(NULL AS STRING) AS EST_QTY
        , CAST(NULL AS STRING) AS MONTH_ACTUAL_IMPS
    {# , NULL AS PUBLISHEDCLASSIFICATION #}

    FROM {{ ref('sat_ad_revenue_showcaseturbo') }}
    LEFT JOIN category_backfill c
        ON
            CAST(c.AD_ADVERTISER_LEGACY AS STRING)
            = CASE
                WHEN AD_BILLING_CUSTOMER_ID = AD_ADVERTISER_ID THEN CAST(AD_BILLING_CUSTOMER_ID AS STRING)
                ELSE CAST(AD_ADVERTISER_ID AS STRING)
            END
    QUALIFY ROW_NUMBER() OVER (PARTITION BY AD_REPORT_HASH ORDER BY EFFECTIVE_FROM DESC) = 1

)

SELECT * FROM satdata

-- showcaseturbo alteryx query
{# -- TABLE IS [dbo].[ShowcaseTurbo]
WITH FirstST AS
(select

	CASE
		when ShowcaseTurbo.[Billing_Customer_ID] = ShowcaseTurbo.[Advertiser_ID] THEN ShowcaseTurbo.[Billing_Customer_ID]
		ELSE ShowcaseTurbo.[Advertiser_ID]
  END AS CustomerSAPNumber,
	ShowcaseTurbo.[Agency_Name] as AgencyName,
	CASE
		WHEN ShowcaseTurbo.[Property] LIKE '%TURBO%' THEN 'TurboSEO' ELSE 'Showcase Plus'
	END AS Type,
	ShowcaseTurbo.[Ad_Unit] as Size,


	CAST(ShowcaseTurbo.[Order_Date] as DATE)as MonthDate,
	CASE
		WHEN ShowcaseTurbo.[Property] LIKE '%TURBO%' THEN 'TurboSEO' ELSE 'Showcase Plus'
	END AS Publication,
	'Paid Advertising (excl House and Contra)' as PaidAdvertising,
	ShowcaseTurbo.[Net_Bill_Amount] as Revenue
FROM ShowcaseTurbo

),

SecondST AS (
Select
	Channel,
	'Turbo/Showcase' as Source,
	CustomerName,
  'xx' as SalesRepTeam,
	AgencyName,
	Type,
	Size,
	Platform,
	ProductType,
	MonthDate,
	Publication,
	PaidAdvertising,
	Revenue,
	CustomerSAPNumber
FROM FirstST

)

Select
Channel,
Source,
CustomerName,
SalesRepTeam,
AgencyName,
Type,
Size,
Platform,
ProductType,
MonthDate,
Publication,
PaidAdvertising,
Revenue,
CustomerSAPNumber as CustomerNumber
from SecondST  #}
