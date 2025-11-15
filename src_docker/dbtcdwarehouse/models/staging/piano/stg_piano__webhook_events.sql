{{
  config(
        tags=['piano']
    )
}}

with source as (
      select * from {{ source('piano', 'piano__webhook_events') }}
)

, renamed as (

    select
        {{ adapter.quote("data") }} as DATA
        , {{ adapter.quote("record_source") }} AS RECORD_SOURCE
        , {{ adapter.quote("load_datetimestamp") }} AS LOAD_DATETIMESTAMP
    from source

)

select * from renamed
