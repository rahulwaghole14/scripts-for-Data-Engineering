{{
  config(
        tags=['braze']
    )
}}

WITH source AS (

    SELECT
        lower(marketing_id) AS external_id  -- pk
        , user_id AS hexa_account_id -- needs to be user_alias
        , subscriber_id AS print__sub_id -- needs to be user_alias
        , CASE
            WHEN country = 'New Zealand' THEN 'NZ'
            WHEN country = 'United States' THEN 'US'
            WHEN country = 'Australia' THEN 'AU'
            ELSE null
        END AS country
        , lower(trim(email)) AS email
        , last_name
        , first_name
        , contact_phone AS phone
        , city_region AS home_city
        , CASE
            WHEN gender IN ('M', 'Male', 'Man') THEN 'M'
            WHEN gender IN ('F', 'Female', 'Woman') THEN 'F'
            WHEN gender IN ('O', 'Non-binary', 'Takatapui') THEN 'O'
            WHEN gender IN ('N', '-1') THEN 'N'
            WHEN gender IN ('P', 'Rather not say', 'Rather-Not-Say') THEN 'P'
            WHEN gender IS NULL OR gender = 'U' THEN NULL
            ELSE NULL
        END AS gender
        , timezone AS time_zone
        , FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_birth AS DATE)) AS dob
        , username AS digital__username

    FROM {{ ref('stg_idm__user_profiles') }}

)

SELECT
    *
    {# row_hash for data-diff #}
    , TO_BASE64(MD5(TO_JSON_STRING(STRUCT(
        external_id
        , hexa_account_id
        , print__sub_id
        , country
        , email
        , last_name
        , first_name
        , phone
        , home_city
        , gender
        , time_zone
        , dob
        , digital__username
    )))) AS row_hash
FROM source
