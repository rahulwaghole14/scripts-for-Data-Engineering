{{
  config(
    tags=['presspatron_api']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('presspatron_api_data', 'subscriptions') }}
)

,renamed AS (
    SELECT
        DISTINCT
        createdAt AS created_at,
        updatedAt AS updated_at,
        subscriptionId AS subscription_id,
        userId AS user_id,
        cancellationAt AS cancellation_at,
        grossAmount AS gross_amount,
        frequency,
        subscriptionStatus AS subscription_status,
        rewardSelected AS reward_selected,
        metadata,
        urlSource AS url_source,
        load_dts
    FROM source
    WHERE subscriptionStatus <> 'subscriptionStatus' AND load_dts <> 'load_dts'
)

,latest_subscriptions AS (
    SELECT
        s.created_at,
        s.updated_at,
        s.subscription_id,
        s.user_id,
        s.cancellation_at,
        s.gross_amount,
        s.frequency,
        s.subscription_status,
        s.reward_selected,
        s.metadata,
        s.url_source,
        s.load_dts
    FROM renamed s
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.load_dts DESC, s.created_at DESC) = 1
)

SELECT * FROM latest_subscriptions
