{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_ContactNumber') }}
)

SELECT
    ObjectPointer
    , TRIM(ContactType) AS ContactType
    , IsMain
    , TRIM(AreaCode) AS AreaCode
    , TRIM(ContactNumber) AS ContactNumber
    , TRIM(Extension) AS Extension
    , TRIM(InternationalCode) AS InternationalCode
    , TRIM(Notes) AS Notes
    , DoNotCall
    , ContactPointer
FROM source
