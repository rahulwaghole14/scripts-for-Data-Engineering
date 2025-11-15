with source as (
      select * from {{ source('adobe_analytics_replatform', 'referrer_type') }}
),
referrer_type as (
    select
        {{ adapter.quote("id") }} as ID,
        {{ adapter.quote("name") }} as NAME,
        {{ adapter.quote("type") }} as TYPE

    from source
)
select * from referrer_type
