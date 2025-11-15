{{
  config(
        tags=['braze']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('acm', 'acm__generic_magazine_print_subs') }}
)

last_run_table AS (
    SELECT MAX(last_run_dts) as updated_at FROM {{ ref('acm__print_sync_last_run') }}
),

, renamed AS (
    SELECT
        LOWER(marketing_id) AS marketing_id
        , internal_name
        , updated_at
    FROM source
    CROSS JOIN last_run_table
)

SELECT * FROM renamed
