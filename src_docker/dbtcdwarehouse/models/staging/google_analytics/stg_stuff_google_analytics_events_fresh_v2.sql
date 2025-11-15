{{
    config(
        tags = ['hexa_google_analytics']
    )
}}

WITH source AS (
    SELECT *, _TABLE_SUFFIX
    FROM {{ source('hexa_google_analytics', 'events_fresh_*') }}

    {% if (target.dataset != 'prod') and (flags.FULL_REFRESH) %}
        WHERE _TABLE_SUFFIX >= '2023111101' AND _TABLE_SUFFIX <= '2023111301'
    {% elif target.dataset != 'prod' %}
        WHERE _TABLE_SUFFIX >= '2023111101' AND _TABLE_SUFFIX <= '2023111102'
    {% elif flags.FULL_REFRESH %}
        -- No filter applied in FULL_REFRESH for prod
    {% else %}
        WHERE _TABLE_SUFFIX > FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
    {% endif %}
),
renamed AS (
    SELECT
        {{ adapter.quote("event_date") }},
        {{ adapter.quote("event_timestamp") }},
        DATETIME(
            TIMESTAMP_MICROS(event_timestamp),
            "Pacific/Auckland"
        ) AS event_timestamp_nz, -- Convert event_timestamp to NZ timezone
        FORMAT_TIMESTAMP('%d%m%Y', TIMESTAMP_MICROS(event_timestamp)) AS event_date_nz, -- Format event date as DDMMYYYY
        {{ adapter.quote("event_name") }},
        {{ adapter.quote("event_params") }},
        {{ adapter.quote("event_previous_timestamp") }},
        {{ adapter.quote("event_value_in_usd") }},
        {{ adapter.quote("event_bundle_sequence_id") }},
        {{ adapter.quote("event_server_timestamp_offset") }},
        {{ adapter.quote("user_id") }},
        {{ adapter.quote("user_pseudo_id") }},
        {{ adapter.quote("privacy_info") }},
        {{ adapter.quote("user_properties") }},
        {{ adapter.quote("user_first_touch_timestamp") }},
        {{ adapter.quote("user_ltv") }},
        {{ adapter.quote("device") }},
        {{ adapter.quote("geo") }},
        {{ adapter.quote("app_info") }},
        {{ adapter.quote("traffic_source") }},
        {{ adapter.quote("stream_id") }},
        {{ adapter.quote("platform") }},
        {{ adapter.quote("event_dimensions") }},
        {{ adapter.quote("ecommerce") }},
        {{ adapter.quote("items") }},
        {{ adapter.quote("collected_traffic_source") }},
        {{ adapter.quote("is_active_user") }},
        {{ adapter.quote("batch_event_index") }},
        {{ adapter.quote("batch_page_id") }},
        {{ adapter.quote("batch_ordering_id") }},
        {{ adapter.quote("session_traffic_source_last_click") }},
        {{ adapter.quote("publisher") }}
    FROM source
    WHERE TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) -- Filter data
)
SELECT *
FROM renamed
