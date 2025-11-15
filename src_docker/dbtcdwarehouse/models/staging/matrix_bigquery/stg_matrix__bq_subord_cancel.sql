{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'subord_cancel') }}
)

SELECT
    sord_pointer
    , CAST(canx_date AS DATE) AS canx_date
    , canx_id
    , request_date
    , syspersonid
    , ObjectPointer
    , SourceID
    , ProductID
    , update_time
FROM source
