{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_period') }}
)

SELECT
  TRIM(PeriodID) AS PeriodID
  , TRIM(PeriodDesc) AS PeriodDesc
  , TRIM(PeriodType) AS PeriodType
  , NumIssuesMonths
  , ChargeCycle
  , FirstMonth
  , FirstDay
FROM source
