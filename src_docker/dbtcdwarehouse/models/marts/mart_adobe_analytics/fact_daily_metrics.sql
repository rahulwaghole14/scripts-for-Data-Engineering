{{ config(
    tags=['adobe'],
    materialized='table',
    on_schema_change='sync_all_columns'
) }}


SELECT
      DAY as day,
      UNIQUE_VISITORS as unique_visitors,
      PAGE_VIEWS as daily_count
FROM {{ ref('fact_digi_audience_hl_total') }}
WHERE DAY >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DAY < CURRENT_DATE('Pacific/Auckland')
ORDER BY DAY
