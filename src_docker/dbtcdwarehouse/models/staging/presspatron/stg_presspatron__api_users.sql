{{
  config(
    tags=['presspatron_api']
  )
}}

WITH source AS (
    SELECT * FROM {{ source('presspatron_api_data', 'users') }}
)

,renamed AS (
    SELECT
      DISTINCT
      createdAt AS created_at,
      updatedAt AS updated_at,
      userId AS user_id,
      newsletterSubscribed AS newsletter_subscribed,
      anonymous,
      firstName AS first_name,
      lastName AS last_name,
      email,
      address,
      load_dts
    FROM source
    WHERE createdAt <> 'createdAt' AND load_dts <> 'load_dts'
)

,latest_users AS (
    SELECT
        created_at,
        updated_at,
        user_id,
        newsletter_subscribed,
        anonymous,
        first_name,
        last_name,
        email,
        address,
        load_dts
    FROM renamed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY load_dts DESC, updated_at DESC) = 1
)

SELECT * FROM latest_users
