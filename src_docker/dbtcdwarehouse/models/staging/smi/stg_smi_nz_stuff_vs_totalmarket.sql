with source as (
      select * from {{ source('smi', 'nz_hexa_vs_totalmarket') }}
),
renamed as (
    select
        {{ adapter.quote("ProductCategory") }},
        {{ adapter.quote("MasterMediaType") }},
        {{ adapter.quote("MediaType") }},
        {{ adapter.quote("MediaSubType") }},
        {{ adapter.quote("State") }},
        {{ adapter.quote("YearMonth") }},
        {{ adapter.quote("Total_Market_Spend") }},
        {{ adapter.quote("hexa_Spend") }},
        {{ adapter.quote("Ingestion_time") }},
        {{ adapter.quote("YYYYMM") }}

    from source
)
select * from renamed
