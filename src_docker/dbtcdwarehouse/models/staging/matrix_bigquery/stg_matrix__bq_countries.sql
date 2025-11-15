{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'tbl_country') }}
)

SELECT
  TRIM(Country_Code) AS country_code,
  AddrRule_ID,
  TRIM(Country_Group) AS country_group,
  TRIM(countryname) AS country_name,
  vat_format,
  Tax_Level1,
  Tax_Level2,
  Tax_Level3,
  Tax_Level4
FROM source
