{{
  config(
    tags = ['adw']
    )
}}

with source as (
      select * from {{ source('naviga', 'naviga__mapping') }}
),
renamed as (
    select
        {{ adapter.quote("generaid") }},
        {{ adapter.quote("legacy_id") }},
        {{ adapter.quote("naviga_id") }}

    from source
)
select * from renamed
