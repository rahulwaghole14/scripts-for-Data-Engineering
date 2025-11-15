{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_product') }}
)

SELECT
  TRIM(ProductID) AS ProductID
  , TRIM(ProductDesc) AS ProductDesc
  , Frequency
FROM source
