{{
    config(
        materialized='incremental',
        tags=['ga'],
        description='Sessions table (ga_session) - Aggregates raw GA4 event data to create session-level metrics',
        partition_by={
            "field": "session_date",
            "data_type": "date",
            "granularity": "day"
        },
        unique_key=['session_id', 'session_date', 'client_id'],
        on_schema_change='ignore', 
    )
}}

-- Query 2: Sessions table (ga_session)
-- This query aggregates raw GA4 event data to create the session-level 'ga_session' table.
-- It groups events by session and calculates various session metrics.

WITH
  events_with_params AS (
    -- First, extract the necessary parameters for sessionization from each event.
    -- IMPORTANT: No filtering based on page_location/page_instance_id here.
    -- We need ALL events to accurately build sessions first.
    SELECT
      event_timestamp,
      user_pseudo_id,
      user_id,
      device,
      geo,
      stream_id,
      app_info.version AS app_version,
      traffic_source,
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
      -- Create the combined session ID at the earliest stage
      CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS combined_session_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_instance_id') AS page_instance_id,
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'profile_id') AS STRING) AS profile_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'system_environment') AS system_environment,
      -- Add detailed traffic source parameters
      session_traffic_source_last_click.manual_campaign.campaign_id AS manual_campaign_id,
      session_traffic_source_last_click.manual_campaign.campaign_name AS manual_campaign_name,
      session_traffic_source_last_click.manual_campaign.medium  AS manual_medium,
      session_traffic_source_last_click.manual_campaign.source  AS manual_source,
      session_traffic_source_last_click.cross_channel_campaign.campaign_name AS cross_channel_campaign_name,
      session_traffic_source_last_click.cross_channel_campaign.source AS cross_channel_campaign_source,
      session_traffic_source_last_click.cross_channel_campaign.medium AS cross_channel_campaign_medium,
      -- Extract click IDs from page_location
      REGEXP_EXTRACT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
        r'[?&]gclid=([^&]+)'
      ) AS gclid,
      REGEXP_EXTRACT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
        r'[?&]dclid=([^&]+)'
      ) AS dclid,
      REGEXP_EXTRACT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
        r'[?&]fbclid=([^&]+)'
      ) AS fbclid
    FROM
      {{ source('hexa_google_analytics', 'events_fresh_*') }}
    WHERE
      {% if is_incremental() %}
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
       {% else %}
        -- For full refresh, use configurable date range
        _TABLE_SUFFIX BETWEEN '{{ var("start_date", "20250701") }}' AND '{{ var("end_date", "20250702") }}'
      {% endif %}
  ),
  -- NEW CTE: Identify ALL combined_session_ids that contain at least ONE event matching the criteria.
  -- This mimics the "session contains event" logic of GA4 segments.
  sessions_with_matching_event AS (
    SELECT DISTINCT combined_session_id
    FROM events_with_params
    WHERE
      session_id IS NOT NULL -- Ensure session_id exists to create a valid combined_session_id
      AND (
        (page_location LIKE '%hexa.co.nz%') -- Original condition
        OR
        -- this ensures hexa android app sessions are included
        (page_location LIKE '%localhost%' AND page_instance_id LIKE 'hexa%')
        OR
        -- this ensures hexa ios app sessions are included
        (
          (page_location IS NULL OR page_location = '(not set)')
          AND page_instance_id LIKE '%hexa%'
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
      -- EXCLUSION CLAUSE:
      AND page_instance_id NOT LIKE '%masthead%'
  ),
  -- NEW CTE: Find the first 'hexa.co.nz' related page view for each session
  first_hexa_page_per_session AS (
    SELECT
      combined_session_id,
      page_location AS first_hexa_page_location,
      page_instance_id AS first_hexa_page_instance_id
    FROM
      events_with_params
    WHERE
      (
        (page_location LIKE '%hexa.co.nz%')
        OR (page_location LIKE '%localhost%' AND page_instance_id LIKE 'hexa%')
        OR ((page_location IS NULL OR page_location = '(not set)') AND page_instance_id LIKE '%hexa%')
        OR (stream_id = '9903815252')
        OR (stream_id = '9903804435')
      )
      AND page_instance_id NOT LIKE '%masthead%' -- Exclude masthead pages
    QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_session_id ORDER BY event_timestamp ASC) = 1
  ),
  session_agg AS (
    -- events data aggregated to the session level.
    SELECT
      user_pseudo_id,
      session_id,
      combined_session_id, -- Include the new combined_session_id for grouping
      MAX(user_id) as user_id,
      MIN(event_date) AS event_date,
      MIN(event_timestamp) AS session_start_timestamp,
      MAX(event_timestamp) AS session_end_timestamp,
      -- Session-level dimensions; should be the same for all events in a session.
      MAX(traffic_source.name) AS visit_campaign,
      MAX(traffic_source.medium) AS visit_medium,
      MAX(traffic_source.source) AS visit_source,
      MAX(stream_id) AS stream_id,
      -- Add aggregated detailed traffic source parameters
      MAX(manual_campaign_id) AS session_manual_campaign_id,
      MAX(manual_campaign_name) AS session_manual_campaign_name,
      MAX(manual_medium) AS session_manual_medium,
      MAX(manual_source) AS session_manual_source,
      MAX(cross_channel_campaign_name) AS session_cross_channel_campaign_name,
      MAX(cross_channel_campaign_source) AS session_cross_channel_campaign_source,
      MAX(cross_channel_campaign_medium) AS session_cross_channel_campaign_medium,
      -- Aggregate click IDs (MAX to get any value if present in the session)
      MAX(gclid) AS session_gclid,
      MAX(dclid) AS session_dclid,
      MAX(fbclid) AS session_fbclid,
      -- Use ARRAY_AGG to get details from the very first event of the session.
      ARRAY_AGG(
        STRUCT(geo, page_location, app_version, page_instance_id)
        ORDER BY
          event_timestamp ASC
        LIMIT
          1
      ) [
      OFFSET
        (0)] AS first_event_details,
      -- User-level dimensions; should be the same for the user.
      MAX(device.category) AS device_category,
      MAX(system_environment) AS system_environment,
      MAX(profile_id) AS profile_id,
      -- Aggregate metrics
      COUNT(*) AS visit_hits,
      COUNT(DISTINCT page_location) AS visit_depth_pages,
      MAX(CAST(session_engaged AS INT64)) AS is_engaged,
      MAX(device.web_info.browser) AS browser,
      MAX(device.mobile_brand_name) AS mobile_device_brand,
      MAX(device.mobile_model_name) AS mobile_device_model
    FROM
      events_with_params -- Use the unfiltered events_with_params here
    WHERE
      session_id IS NOT NULL
    GROUP BY
      user_pseudo_id,
      session_id,
      combined_session_id -- Group by the new combined_session_id
  )
SELECT
  -- Core columns
  s.event_date AS session_date,
  s.user_pseudo_id AS client_id,
  s.user_id AS user_id,
  -- Use the combined session_id
  s.combined_session_id AS session_id,
  TIMESTAMP_MICROS(s.session_start_timestamp) AS visit_start_time_utc,
  TIMESTAMP_MICROS(s.session_end_timestamp) AS visit_end_time_utc,
  s.visit_campaign AS first_acquisition_campaign,
  s.visit_medium AS first_acquisition_medium,
  s.visit_source AS first_acquisition_source,
  s.visit_source,
  s.visit_medium,
  s.visit_campaign,
  SAFE_DIVIDE(s.session_end_timestamp - s.session_start_timestamp, 1000000) AS visit_duration,
  s.visit_depth_pages,
  -- A session is a bounce if it was not engaged.
  CASE WHEN s.is_engaged = 1 THEN 0 ELSE 1 END AS visit_bounces,
  s.visit_hits,
  -- Prioritize the first 'hexa' page as the landing page, fallback to actual first event if no 'hexa' page is found.
  COALESCE(fsps.first_hexa_page_location, s.first_event_details.page_location) AS visit_landing_page,
  COALESCE(fsps.first_hexa_page_instance_id, s.first_event_details.page_instance_id) AS visit_landing_page_instance_id,
  s.first_event_details.geo.continent AS geo_continent,
  s.first_event_details.geo.country AS geo_country,
  s.first_event_details.geo.region AS geo_region,
  s.first_event_details.geo.city AS geo_city,
  s.first_event_details.geo.metro AS geo_metro,
  s.stream_id,
  s.first_event_details.app_version AS app_version,
  -- use system_environment in place of platform as its most accurate
  s.system_environment,
  -- User property columns
  s.device_category,
  s.profile_id,
  -- Add requested session_traffic_source_last_click columns with correct naming
  s.session_manual_campaign_id AS session_traffic_source_last_click_manual_campaign_campaign_id,
  s.session_manual_campaign_name AS session_traffic_source_last_click_manual_campaign_campaign_name,
  s.session_manual_medium AS session_traffic_source_last_click_manual_campaign_medium,
  s.session_manual_source AS session_traffic_source_last_click_manual_campaign_source,
  s.session_cross_channel_campaign_name AS session_traffic_source_last_click_cross_channel_campaign_name,
  s.session_cross_channel_campaign_source AS session_traffic_source_last_click_cross_channel_campaign_source,
  s.session_cross_channel_campaign_medium AS session_traffic_source_last_click_cross_channel_campaign_medium,
  -- click ID columns
  s.session_gclid AS cd_gclid,
  s.session_dclid AS cd_dclid,
  s.session_fbclid AS cd_fbclid,
  s.browser,
  s.mobile_device_model,
  s.mobile_device_brand,
  null as is_bounce -- Placeholder for is_bounce, and it will be updated when next day's schedule query runs.
FROM
  session_agg s
INNER JOIN
  sessions_with_matching_event swme ON s.combined_session_id = swme.combined_session_id
LEFT JOIN
  first_hexa_page_per_session fsps ON s.combined_session_id = fsps.combined_session_id