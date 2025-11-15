{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_person') }}
)

SELECT
    person_pointer
    , TRIM(title) AS title
    , TRIM(FirstName) AS FirstName
    , TRIM(SurName) AS SurName
    , ObjectPointer
    , NULLIF(TRIM(PersContact3), '') AS PersContact3  -- Set PersContact3 to NULL if blank
FROM source
