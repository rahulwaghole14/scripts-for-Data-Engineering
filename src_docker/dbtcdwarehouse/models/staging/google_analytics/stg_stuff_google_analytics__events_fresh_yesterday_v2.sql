{{
    config(
        tags=['hexa_google_analytics']
    )
}}

SELECT
    *,
    DATETIME(TIMESTAMP_MICROS(event_timestamp), "Pacific/Auckland") AS event_timestamp_nz, -- Convert event_timestamp to NZ timezone
    FORMAT_TIMESTAMP('%d%m%Y', TIMESTAMP_MICROS(event_timestamp)) AS event_date_nz -- Format event_timestamp to DDMMYYYY
FROM {{ source('hexa_google_analytics', 'events_fresh_*') }}