{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'subscriber') }}
)

SELECT
  hash_key,
  TRIM(subs_id) AS subs_id ,
  subs_loc_pointer,
  subs_perid,
  subs_renew_loc_ptr,
  subs_renewalid,
  TRIM(subtype_id) AS subtype_id ,
  timestamp,
  subs_pointer,
  TRIM(tax_exempt) AS tax_exempt,
  TRIM(tax_rate_id) AS tax_rate_id,
  ObjectPointer,
  TRIM(TaxExemptID) AS TaxExemptID,
  update_time
FROM source
