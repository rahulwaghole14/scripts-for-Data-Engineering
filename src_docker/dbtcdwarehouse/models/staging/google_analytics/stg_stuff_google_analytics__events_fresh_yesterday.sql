{{
    config(
        tags = ['hexa_google_analytics']
    )
}}

{%- if (target.dataset != 'prod') -%}
    SELECT
        *,
        DATETIME(TIMESTAMP_MICROS(event_timestamp), "Pacific/Auckland") AS event_timestamp_nz -- Convert event_timestamp to NZ timezone
    FROM {{ source('hexa_google_analytics_dev', 'events_fresh_*') }}
    WHERE _TABLE_SUFFIX = FORMAT_DATE(
            '%Y%m%d'
            , DATE_SUB(
                CURRENT_DATE('Pacific/Auckland')
                , INTERVAL 1 DAY
            )
        )
    AND EXISTS (
        SELECT 1
        FROM UNNEST(event_params) AS ep
        WHERE ep.key = 'system_environment' AND ep.value.string_value = 'web'
    )
{%- else -%}
    SELECT
        *,
        DATETIME(TIMESTAMP_MICROS(event_timestamp), "Pacific/Auckland") AS event_timestamp_nz -- Convert event_timestamp to NZ timezone
    FROM {{ source('hexa_google_analytics', 'events_fresh_*') }}
    WHERE _TABLE_SUFFIX = FORMAT_DATE(
        '%Y%m%d'
        , DATE_SUB(
            CURRENT_DATE('Pacific/Auckland')
            , INTERVAL 1 DAY
        )
    )
{% endif %}
