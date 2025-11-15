{{
  config(
    materialized = 'table',
    tags = 'adw',
    partition_by = {
      "field": "DATE",
      "data_type": "date",
      "granularity": "day"
    },
    )
}}

-- update before 8am

with adrevenuedata as (

    select * from {{ ref('int_ad_revenue__merge') }}

)

select * from adrevenuedata
