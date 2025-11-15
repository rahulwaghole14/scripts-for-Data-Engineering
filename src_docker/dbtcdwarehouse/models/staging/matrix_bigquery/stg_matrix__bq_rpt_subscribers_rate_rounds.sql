{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'ffx_vw_RPT_SubsRateRounds') }}
)

SELECT
    TRIM(Rate_head_id) AS Rate_head_id
    , TRIM(Round_id) AS round_id
    , TRIM(Subs_id) AS subs_id
    , TRIM(Product_id) AS product_id
FROM source
