{{
    config(
        materialized='incremental',
        tags=['ga'],
        description='Granular GA4 events table with extracted parameters, partitioned by event_date',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        unique_key=['client_id', 'event_name', 'session_id', 'event_timestamp', 'cd_component_id','cd_page_id', 'cd_page_instance_id','cd_custom_event_name'],
        on_schema_change='ignore',
    )
}}

-- Query 1: Granular events table (stg_ga_view)
-- This query selects and transforms raw GA4 event data into the 'stg_ga_view' structure.

WITH
  events_with_extracted_params AS (
    -- First, extract all necessary parameters and properties from the nested fields.
    -- Using a CTE makes the final SELECT statement cleaner.
    SELECT
      *,
      -- Extract event parameters using a subquery for each key
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS p_engagement_time_msec,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS p_page_location,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_instance_id') AS p_page_instance_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_type') AS p_page_type,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_id') AS p_page_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_sensitivity') AS p_page_sensitivity,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_entities') AS p_page_entities,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'custom_event_name') AS p_custom_event_name,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_primary_category') AS p_page_primary_category,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_secondary_category') AS p_page_secondary_category,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'topics') AS p_topics,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'author') AS p_author,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_keywords') AS p_page_keywords,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'publication_channel') AS p_publication_channel,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'component_id') AS p_component_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'component_type') AS p_component_type,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'component_location') AS p_component_location,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'component_section') AS p_component_section,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS p_session_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS p_session_engaged,
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') AS p_event_value,
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'profile_id') AS STRING) AS p_profile_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'system_environment') AS p_system_environment,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_name') AS p_page_name,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'video_event') AS p_video_event,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'video_title') AS p_video_title
    FROM
      {{ source('hexa_google_analytics', 'events_fresh_*') }} -- Using dbt source reference
    WHERE
      {% if is_incremental() %}
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      {% else %}
        -- For full refresh, use configurable date range
        _TABLE_SUFFIX BETWEEN '{{ var("start_date", "20250701") }}' AND '{{ var("end_date", "20250702") }}'
      {% endif %}
      AND (
        -- page_location contains hexa.co.nz
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%hexa.co.nz%'
        OR
        -- this ensures hexa android app events are included
        (
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%localhost%'
          AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_instance_id') LIKE 'hexa%'
        )
        OR
        (
          stream_id = '9903815252'
        )
        OR
        (
          stream_id = '9903804435'
        )
      )
  ),
  
  deduplicated_events AS (
    -- Remove duplicates based on the unique key fields
    -- Using ROW_NUMBER() to keep the latest record for each unique combination
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY 
          user_pseudo_id,
          event_name,
          CONCAT(user_pseudo_id, '-', CAST(p_session_id AS STRING)),
          event_timestamp,
          p_component_id,
          p_page_instance_id,
          p_custom_event_name,
          p_page_id
        ORDER BY 
          event_timestamp DESC,
          event_bundle_sequence_id DESC
      ) AS row_num
    FROM events_with_extracted_params
  )
  
SELECT
  -- Core columns
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  event_name,
  user_pseudo_id AS client_id,
  user_id,
  CONCAT(user_pseudo_id, '-', CAST(p_session_id AS STRING)) AS session_id,
  CASE WHEN p_session_engaged = '1' THEN TRUE ELSE FALSE END AS session_engaged,
  p_engagement_time_msec AS user_engagement_msec,
  event_bundle_sequence_id,
  traffic_source.name AS first_acquisition_campaign,
  traffic_source.medium AS first_acquisition_medium,
  traffic_source.source AS first_acquisition_source,
  collected_traffic_source.manual_source AS event_source,
  collected_traffic_source.manual_medium AS event_medium,
  collected_traffic_source.manual_campaign_name AS event_campaign,
  geo.continent AS geo_continent,
  geo.country AS geo_country,
  geo.region AS geo_region,
  geo.city AS geo_city,
  geo.metro AS geo_metro,
  NET.HOST(p_page_location) AS page_urlhost,
  COALESCE(event_value_in_usd, p_event_value) AS event_value,
  stream_id,
  app_info.version AS app_version,
  -- use system_environment in place of platform as its most accurate
  p_system_environment AS cd_system_environment,
  -- Event param columns (cd_)
  p_page_location AS cd_page_location,
  p_page_instance_id AS cd_page_instance_id,
  p_page_type AS cd_page_type,
  p_page_id AS cd_page_id,
  p_page_sensitivity AS cd_page_sensitivity,
  p_page_entities AS cd_page_entities,
  p_custom_event_name AS cd_custom_event_name,
  p_page_primary_category AS cd_page_primary_category,
  p_page_secondary_category AS cd_page_secondary_category,
  p_topics AS cd_topics,
  p_author AS cd_author,
  p_page_keywords AS cd_page_keywords,
  p_publication_channel AS cd_publication_channel,
  p_component_id AS cd_component_id,
  p_component_type AS cd_component_type,
  p_component_location AS cd_component_location,
  p_component_section AS cd_component_section,
  device.category AS device_category,
  p_profile_id AS profile_id,
  device.web_info.browser AS browser,
  device.mobile_model_name AS mobile_device_model,
  device.mobile_brand_name AS mobile_device_brand,
  p_page_name AS cd_page_name,
  p_video_event AS cd_video_event,
  p_video_title AS cd_video_title
FROM
  deduplicated_events
WHERE
  row_num = 1