with source as (
      select * from {{ source('smi', 'nz_hexa_mo_vs_category') }}
),
renamed as (
    select
        {{ adapter.quote("MediaType") }},
        {{ adapter.quote("ProductCategory") }},
        {{ adapter.quote("MasterOwner") }},
        {{ adapter.quote("YearMonth") }},
        {{ adapter.quote("GrossMediaSpend") }},
        {{ adapter.quote("CalYear") }},
        {{ adapter.quote("CalYear-Quarter") }},
        {{ adapter.quote("CalYear-Month") }},
        {{ adapter.quote("CalQuarter") }},
        {{ adapter.quote("CalMonth") }},
        {{ adapter.quote("Ingestion_time") }},
        {{ adapter.quote("YYYYMM") }}

    from source
)
select * from renamed
