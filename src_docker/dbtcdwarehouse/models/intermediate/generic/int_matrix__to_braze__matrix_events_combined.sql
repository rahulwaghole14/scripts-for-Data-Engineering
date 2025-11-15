{{
  config(
    tags='bq_matrix',
    materialized='incremental',
    unique_key=['external_id', 'name', 'event_time'],
    on_schema_change='append'
  )
}}

WITH combined_results AS (
    SELECT
        external_id,
        event_name AS name,
        event_time,
        properties
    FROM {{ ref('int_matrix__to_braze__matrix_events_cancellation') }}

UNION ALL

SELECT
    external_id,
    event_name AS name,
    event_time,
    properties
FROM {{ ref('int_matrix__to_braze__matrix_events_complaints') }}

UNION ALL

SELECT
    external_id,
    event_name AS name,
    event_time,
    properties
FROM {{ ref('int_matrix__to_braze__matrix_events_purchase') }}

UNION ALL

SELECT
    external_id,
    event_name AS name,
    event_time,
    properties
FROM {{ ref('int_matrix__to_braze__matrix_events_winback') }}
)

SELECT
    external_id,
    name,
    COALESCE(event_time, CURRENT_TIMESTAMP()) AS event_time,
    TO_JSON(
        STRUCT(
            name,
            COALESCE(event_time, CURRENT_TIMESTAMP()) AS time,
            properties
    )) AS PAYLOAD,
    CURRENT_TIMESTAMP() AS updated_at
FROM combined_results

{% if is_incremental() %}
    WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})  -- Only add new events
{% endif %}
