{{
  config(
    tags = ['bonzai']
  )
}}

with source as (
      select * from {{ source('bonzai', 'data_*') }}
),
renamed as (
    select
        {{ adapter.quote("date") }},
        {{ adapter.quote("campaign_id") }},
        {{ adapter.quote("campaign_name") }},
        {{ adapter.quote("ad_id") }},
        {{ adapter.quote("ad_name") }},
        {{ adapter.quote("ad_format") }},
        {{ adapter.quote("placement_id") }},
        {{ adapter.quote("placement_name") }},
        {{ adapter.quote("requested_impressions") }},
        {{ adapter.quote("served_impressions") }},
        {{ adapter.quote("clicks") }},
        {{ adapter.quote("engagement") }},
        {{ adapter.quote("video_time_spent_total") }},
        {{ adapter.quote("autoplays") }},
        {{ adapter.quote("autoplays_quartile_one") }},
        {{ adapter.quote("autoplays_quartile_two") }},
        {{ adapter.quote("autoplays_quartile_three") }},
        {{ adapter.quote("auto_complete_play") }},
        {{ adapter.quote("auto_completion_rate") }},
        {{ adapter.quote("total_seconds_watched") }},
        {{ adapter.quote("video_plays___user_initiated") }},
        {{ adapter.quote("user_initiated_quartile_one") }},
        {{ adapter.quote("user_initiated_quartile_two") }},
        {{ adapter.quote("user_initiated_quartile_three") }},
        {{ adapter.quote("user_initiated_user_completion") }},
        {{ adapter.quote("user_initiated_video_completion_rate") }},
        {{ adapter.quote("total_seconds_watched_by_user") }},
        {{ adapter.quote("total_video_clicks") }}

    from source
)
select * from renamed
