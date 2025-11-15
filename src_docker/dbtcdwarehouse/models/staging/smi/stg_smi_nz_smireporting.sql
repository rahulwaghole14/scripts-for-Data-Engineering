with source as (
      select * from {{ source('smi', 'nz_smireporting') }}
),
renamed as (
    select
        {{ adapter.quote("Media") }},
        {{ adapter.quote("MasterMediaType") }},
        {{ adapter.quote("MediaType") }},
        {{ adapter.quote("MediaSubType") }},
        {{ adapter.quote("Country") }},
        {{ adapter.quote("State") }},
        {{ adapter.quote("MarketType") }},
        {{ adapter.quote("Market") }},
        {{ adapter.quote("Network") }},
        {{ adapter.quote("MasterOwner") }},
        {{ adapter.quote("Owner") }},
        {{ adapter.quote("SalesRep") }},
        {{ adapter.quote("TargetAudience") }},
        {{ adapter.quote("Genre") }},
        {{ adapter.quote("YearMonth") }},
        {{ adapter.quote("GrossMediaSpend") }},
        {{ adapter.quote("CalYear") }},
        {{ adapter.quote("CalYear-Quarter") }},
        {{ adapter.quote("CalYear-Month") }},
        {{ adapter.quote("CalQuarter") }},
        {{ adapter.quote("CalMonth") }},
        {{ adapter.quote("MasterNetwork") }},
        {{ adapter.quote("Ingestion_time") }},
        {{ adapter.quote("YYYYMM") }}

    from source
)
select * from renamed
