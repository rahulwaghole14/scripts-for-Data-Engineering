{{
  config(
    tags=['bonzai'],
    materialized='incremental',
    on_schema_change='sync_all_columns',
    partition_by={
      "field": "DATE",
      "data_type": "DATE",
      "granularity": "day"
    }
  )
}}

WITH renamed AS (
    SELECT
        {{ adapter.quote("date") }} AS DATE,
        {{ adapter.quote("campaign_id") }} AS CAMPAIGN_ID,
        {{ adapter.quote("campaign_name") }} AS CAMPAIGN_NAME,
        {{ adapter.quote("ad_id") }} AS AD_ID,
        {{ adapter.quote("ad_name") }} AS AD_NAME,
        {{ adapter.quote("ad_format") }} AS AD_FORMAT,
        {{ adapter.quote("placement_id") }} AS PLACEMENT_ID,
        {{ adapter.quote("placement_name") }} AS PLACEMENT_NAME,
        {{ adapter.quote("requested_impressions") }} AS REQUESTED_IMPRESSIONS,
        {{ adapter.quote("served_impressions") }} AS SERVED_IMPRESSIONS,
        {{ adapter.quote("clicks") }} AS CLICKS,
        {{ adapter.quote("engagement") }} AS ENGAGEMENT,
        {{ adapter.quote("video_time_spent_total") }} AS VIDEO_TIME_SPENT_TOTAL,
        {{ adapter.quote("autoplays") }} AS AUTOPLAYS,
        {{ adapter.quote("autoplays_quartile_one") }} AS AUTOPLAYS_QUARTILE_ONE,
        {{ adapter.quote("autoplays_quartile_two") }} AS AUTOPLAYS_QUARTILE_TWO,
        {{ adapter.quote("autoplays_quartile_three") }} AS AUTOPLAYS_QUARTILE_THREE,
        {{ adapter.quote("auto_complete_play") }} AS AUTO_COMPLETE_PLAY,
        {{ adapter.quote("auto_completion_rate") }} AS AUTO_COMPLETION_RATE,
        {{ adapter.quote("total_seconds_watched") }} AS TOTAL_SECONDS_WATCHED,
        {{ adapter.quote("video_plays___user_initiated") }} AS VIDEO_PLAYS_USER_INITIATED,
        {{ adapter.quote("user_initiated_quartile_one") }} AS USER_INITIATED_QUARTILE_ONE,
        {{ adapter.quote("user_initiated_quartile_two") }} AS USER_INITIATED_QUARTILE_TWO,
        {{ adapter.quote("user_initiated_quartile_three") }} AS USER_INITIATED_QUARTILE_THREE,
        {{ adapter.quote("user_initiated_user_completion") }} AS USER_INITIATED_USER_COMPLETION,
        {{ adapter.quote("user_initiated_video_completion_rate") }} AS USER_INITIATED_VIDEO_COMPLETION_RATE,
        {{ adapter.quote("total_seconds_watched_by_user") }} AS TOTAL_SECONDS_WATCHED_BY_USER,
        {{ adapter.quote("total_video_clicks") }} AS TOTAL_VIDEO_CLICKS

    FROM {{ ref('stg_bonzai_data') }}
)

SELECT * FROM renamed
