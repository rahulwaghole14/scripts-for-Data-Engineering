{{ config(
    tags=['adobe'],
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    DAY ,
    UNIQUE_VISITORS ,
    PAGE_VIEWS
FROM {{ ref('fact_digi_audience_hl_total') }}
WHERE DAY >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
