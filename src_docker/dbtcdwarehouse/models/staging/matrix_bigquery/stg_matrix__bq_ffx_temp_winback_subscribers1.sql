{{
  config(
    tags=['bq_matrix']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bq_matrix', 'ffx_temp_winback_subscribers1') }}
)

, renamed AS (
    SELECT
        Sub_Id AS sub_id
        , Proposed_Rate AS proposed_rate
        , ProductID AS product_id
        , RateID AS rate_id
        , Cancellation_Date AS cancellation_date
        , Week_No AS week_no
        , Cancelled_Pubs AS cancelled_pubs
        , Cancelled_Proposed_Rate AS cancelled_proposed_rate
        , Cancelled_Multiple_Pubs AS cancelled_multiple_pubs
        , Cancellation_ID AS cancellation_id
        , Sord_Startdate  AS sord_startdate
        , Service AS service
    FROM source
)

SELECT * FROM renamed
