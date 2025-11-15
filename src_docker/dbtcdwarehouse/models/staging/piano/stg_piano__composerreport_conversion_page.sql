{{
  config(
        tags=['piano']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('piano', 'piano__composerreport_conversion_page') }}
),
renamed AS (
    SELECT
        {{ adapter.quote("URL") }},
        {{ adapter.quote("Exposures") }},
        {{ adapter.quote("Conversions") }},
        {{ adapter.quote("Conversion_rate") }},
        {{ adapter.quote("Currency") }},
        {{ adapter.quote("Value") }},
        {{ adapter.quote("date_at") }},
        {{ adapter.quote("app_id") }},
        {{ adapter.quote("app_name") }},
        {{ adapter.quote("report_type") }}
    FROM source
)
SELECT *
FROM renamed
