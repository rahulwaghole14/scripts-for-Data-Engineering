{{
  config(
        tags=['braze']
    )
}}

with source as (
      select * from {{ source('acm', 'acm__print_data_blacklist') }}
),

last_run_table AS (
    SELECT MAX(last_run_dts) as acm_last_run FROM {{ ref('acm__print_sync_last_run') }}
),

renamed as (
    select
        {{ adapter.quote("index") }},
        {{ adapter.quote("address") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("error_reason") }},
        {{ adapter.quote("update_status") }},
        LOWER({{ adapter.quote("marketing_id") }}) AS marketing_id,
        GREATEST(
            COALESCE(PARSE_TIMESTAMP('%Y/%m/%d %H:%M:%S', update_status), acm_last_run),
            acm_last_run
        ) AS updated_at
    from source
    CROSS JOIN last_run_table
)
select * from renamed
