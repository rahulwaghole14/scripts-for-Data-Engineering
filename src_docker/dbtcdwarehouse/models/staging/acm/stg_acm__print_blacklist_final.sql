{{
  config(
        tags=['braze']
    )
}}

with source as (
      select * from {{ source('acm', 'acm__print_blacklist_final') }}
),

last_run_table AS (
    SELECT MAX(last_run_dts) as updated_at FROM {{ ref('acm__print_sync_last_run') }}
),

renamed as (
    select
        LOWER({{ adapter.quote("marketing_id") }}) AS marketing_id
        , updated_at
    from source
    CROSS JOIN last_run_table
)

select * from renamed
