{{
  config(
    tags=['presspatron_api']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('presspatron_api_data', 'transactions') }}
)

, renamed AS (
    SELECT
        CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', createdAt) AS DATE) AS created_at_date
        , CAST(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', updatedAt) AS DATE) AS updated_at_date
        , createdAt AS created_at
        , updatedAt AS updated_at
        , transactionId AS transaction_id
        , grossAmount AS gross_amount
        , cardIssueCountry AS card_issue_country
        , userId AS user_id
        , subscriptionId AS subscription_id
        , frequency
        , processorCreditCardFee AS processor_credit_card_fee
        , netAmount AS net_amount
        , totalFees AS total_fees
        , processorBankTransferFee AS processor_bank_transfer_fee
        , pressPatronCommission AS press_patron_commission
        , rewardSelected AS reward_selected
        , metadata
        , urlSource AS url_source
        , paymentStatus AS payment_status
        , pressPatronCommissionSalesTax AS press_patron_commission_sales_tax
        , CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', SUBSTR(load_dts, 1, 19)) AS DATE) AS load_dts_date
        , load_dts
        , ROW_NUMBER() OVER (
            PARTITION BY transactionId
            ORDER BY load_dts DESC, updatedAt DESC
        ) AS row_num
    FROM source
    WHERE createdAt <> 'createdAt' AND load_dts <> 'load_dts' AND CAST(netAmount AS NUMERIC) > 0
)
,sign_up_date AS (
  SELECT 
  user_id
  ,MIN(created_at) AS sign_up_date
  FROM renamed
  GROUP BY user_id
)
, unique_transactions AS (
    SELECT
        created_at
        , updated_at
        , transaction_id
        , gross_amount
        , card_issue_country
        , user_id
        , subscription_id
        , frequency
        , processor_credit_card_fee
        , net_amount
        , total_fees
        , processor_bank_transfer_fee
        , press_patron_commission
        , reward_selected
        , metadata
        , url_source
        , payment_status
        , press_patron_commission_sales_tax
        , load_dts
        , ROW_NUMBER() OVER (
            PARTITION BY
                created_at_date, updated_at_date, card_issue_country, user_id, transaction_id, subscription_id, frequency
                , processor_credit_card_fee, net_amount, total_fees, processor_bank_transfer_fee
                , press_patron_commission, reward_selected, metadata, url_source, payment_status
                , press_patron_commission_sales_tax, load_dts_date
            ORDER BY load_dts_date DESC, created_at_date DESC
        ) AS row_num
    FROM renamed
    WHERE row_num = 1
)


SELECT
     sign_up_date.sign_up_date
    ,unique_transactions.*
FROM
    unique_transactions
LEFT JOIN sign_up_date
ON unique_transactions.user_id = sign_up_date.user_id
WHERE
    row_num = 1
