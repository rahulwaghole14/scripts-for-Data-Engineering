{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'subscription') }}
    )


SELECT
    source.ObjectPointer
     , TRIM(tbl_product.ProductID) AS ProductID
     , TRIM(source.period_id) AS period_id
     , TRIM(source.ServiceID) AS ServiceID
     , TRIM(source.sord_id) AS sord_id
     , CAST(CAST(source.sord_entry_date AS TIMESTAMP) AS DATE) AS sord_entry_date
     , CAST(CAST(source.sord_startdate AS TIMESTAMP) AS DATE) AS sord_startdate
     , CAST(source.sord_stopdate AS DATE) AS sord_stopdate
     , TRIM(source.sponsor_ref) AS sponsor_ref
     , source.subs_pointer
     , source.sord_pointer
     , TRIM(source.order_type) AS order_type
     , CAST(source.exp_end_date AS DATE) exp_end_date
     , CAST(source.last_invoiced AS DATE) last_invoiced
     , TRIM(source.dist_type) AS dist_type
     , TRIM(source.camp_id) AS camp_id
     , CAST(source.PaidThruDate AS TIMESTAMP) AS PaidThruDate
     , source.CurPeriod
     , source.AccountPointer
     , TRIM(source.cost_centre) AS cost_centre
     , source.init_grace_issues
     , TRIM(source.invoice_flag) AS invoice_flag
     , TRIM(source.paytype_id) AS paytype_id
     , TRIM(source.SourceID) AS SourceID
     , TRIM(source.sysperson_person) AS sysperson_person
     , GREATEST(
        COALESCE(source.update_time, TIMESTAMP '1970-01-01'),
        COALESCE(rate_header.update_time, TIMESTAMP '1970-01-01'),
        COALESCE(rate_structure.update_time, TIMESTAMP '1970-01-01'),
        COALESCE(rate_structure_item.update_time, TIMESTAMP '1970-01-01'),
        COALESCE(rate_item.update_time, TIMESTAMP '1970-01-01'),
        COALESCE(tbl_product.update_time, TIMESTAMP '1970-01-01')
       ) AS update_time
FROM source
         INNER JOIN {{ source('bq_matrix', 'rate_header') }} rate_header
ON trim(source.rate_head_id) = trim(rate_header.rate_head_id)
    INNER JOIN {{ source('bq_matrix', 'tbl_RateStructure') }} rate_structure
    ON
    trim(rate_header.rate_head_id) = trim(rate_structure.RateHeadID)
    AND (rate_structure.ToDate IS null OR CAST(rate_structure.ToDate AS DATE) >= CURRENT_DATE('Pacific/Auckland'))
    INNER JOIN {{ source('bq_matrix', 'tbl_RateStructureItem') }} rate_structure_item
    ON rate_structure.RSPointer = rate_structure_item.RSPointer
    INNER JOIN {{ source('bq_matrix', 'tbl_RateItem') }} rate_item
    ON trim(rate_structure_item.RateItemID) = trim(rate_item.RateItemID)
    INNER JOIN {{ source('bq_matrix', 'tbl_product') }} tbl_product
    ON trim(tbl_product.ProductID) = trim(rate_item.ProductID)