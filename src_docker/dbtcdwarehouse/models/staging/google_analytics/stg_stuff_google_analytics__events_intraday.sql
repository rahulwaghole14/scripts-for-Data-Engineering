{{
    config(
        tags = ['hexa_google_analytics']
    )
}}

{%- if (target.dataset != 'prod') -%}
SELECT * FROM {{ source('hexa_google_analytics_dev', 'events_intraday_*') }}
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE('Pacific/Auckland'))
AND EXISTS (
    SELECT 1
    FROM UNNEST(event_params) AS ep
    WHERE ep.key = 'system_environment' AND ep.value.string_value = 'web'
)
{%- else -%}
SELECT * FROM {{ source('hexa_google_analytics', 'events_intraday_*') }}
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE('Pacific/Auckland'))
{% endif %}
