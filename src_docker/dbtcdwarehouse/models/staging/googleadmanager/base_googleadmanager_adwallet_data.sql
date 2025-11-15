with source as (
      select * from {{ source('googleadmanager', 'adwallet_data') }}
),
renamed as (
    select
        {{ adapter.quote("screenshotsURLs") }},
        {{ adapter.quote("screenshotURL") }},
        {{ adapter.quote("extraAssets") }},
        {{ adapter.quote("creative") }},
        {{ adapter.quote("lineItem") }},
        {{ adapter.quote("takenAtURL") }},
        {{ adapter.quote("order") }},
        {{ adapter.quote("device") }},
        {{ adapter.quote("timestamp") }}

    from source
)
select * from renamed
