{# {{
  config(
    tags=['bq_matrix']
  )
}}

with source as (
      select * from {{ source('bq_matrix', 'ffx_tbl_RPT_TM1retail') }}
),
renamed as (
    select
        {{ adapter.quote("Retail_ProductID") }},
        {{ adapter.quote("Retail_SaleDate") }},
        {{ adapter.quote("Retail_RegionCode") }},
        {{ adapter.quote("Retail_RegionDesc") }},
        {{ adapter.quote("Retail_DistrictCode") }},
        {{ adapter.quote("Retail_DistrictDesc") }},
        {{ adapter.quote("Retail_SuburbCode") }},
        {{ adapter.quote("Retail_SuburbDesc") }},
        {{ adapter.quote("Retail_QtyDelivered") }},
        {{ adapter.quote("Retail_QtyReturned") }},
        {{ adapter.quote("Retail_QtySales") }},
        {{ adapter.quote("Retail_ValueSales") }},
        {{ adapter.quote("Retail_Frees") }},
        {{ adapter.quote("Retail_FreesValue") }},
        {{ adapter.quote("Retail_Paid") }},
        {{ adapter.quote("Retail_PaidValue") }},
        {{ adapter.quote("Retail_Flats") }},
        {{ adapter.quote("Retail_Rolls") }},
        {{ adapter.quote("Retail_TotalBundles") }},
        {{ adapter.quote("Retail_RouteID") }},
        {{ adapter.quote("Retail_TruckRoute") }},
        {{ adapter.quote("Retail_AllRoutes") }},
        {{ adapter.quote("Retail_RetailerID") }},
        {{ adapter.quote("Retail_RetailerName") }},
        {{ adapter.quote("update_time") }}

    from source
)
select * from renamed #}
