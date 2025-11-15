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

SELECT
  fact_ad_revenue.*
  , dim_date.CalQuarterName
  , dim_date.CalYearKey
  , dim_date.CalQuarterKey
FROM {{ ref('fact_ad_revenue') }} fact_ad_revenue
LEFT JOIN {{ ref('dim_date') }} dim_date ON (fact_ad_revenue.DATE = PARSE_DATE('%Y%m%d', CAST(dim_date.DimDateKey AS STRING)))
