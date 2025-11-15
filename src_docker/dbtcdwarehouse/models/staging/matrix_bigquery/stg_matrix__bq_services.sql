{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_service') }}
)

SELECT
  TRIM(ServiceID) AS ServiceID
  , TRIM(ServiceDesc) AS ServiceDesc
  , NumDays
  , Mon
  , Tue
  , Wed
  , Thu
  , Fri
  , Sat
  , Sun
  , MinIssuesCredit
FROM source
