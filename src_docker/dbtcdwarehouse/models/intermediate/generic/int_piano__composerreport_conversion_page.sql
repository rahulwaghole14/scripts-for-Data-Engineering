{{
  config(
    tags=['piano'],
    materialized='table',
    on_schema_change='sync_all_columns',
  )
}}

WITH conversion AS (
    SELECT
        URL,
        Exposures,
        Conversions,
        Conversion_rate,
        Currency,
        Value,
        date_at,
        -- Convert `date_at` to New Zealand Timezone and format it as mmddyyyy
        DATE(TIMESTAMP(date_at), 'Pacific/Auckland') AS date_at_nzt,
        app_id,
        app_name,
        report_type,

    FROM {{ ref('stg_piano__composerreport_conversion_page') }}
)

SELECT * FROM conversion
