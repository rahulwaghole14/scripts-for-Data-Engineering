{{
  config(
        tags=['piano']
    )
}}

with source as (
      select * from {{ source('piano', 'piano__vxconversionreport_conversion_page') }}
),
renamed as (
    select
        {{ adapter.quote("URL") }},
        {{ adapter.quote("Exposures") }},
        {{ adapter.quote("Conversions") }},
        {{ adapter.quote("Conversion_rate") }},
        {{ adapter.quote("Currency") }},
        {{ adapter.quote("Value") }},
        {{ adapter.quote("date_at") }},
        {{ adapter.quote("app_id") }},
        {{ adapter.quote("app_name") }},
        {{ adapter.quote("report_type") }}

    from source
)
select * from renamed
