{{ config(
    tags = ['braze_sync_mart'],
    partition_by={
      "field": "UPDATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH country_codes AS (

    SELECT REGEXP_REPLACE(LOWER(TRIM(country_name)), r'[^a-z ]', '') as country_name
        , alpha_2_code
    FROM {{ ref('country_codes') }}

)

, users AS (

    SELECT *
        , (
            SELECT
                REGEXP_REPLACE(LOWER(TRIM(JSON_VALUE(addresses_json, '$.country'))), r'[^A-Za-z ]', '')
            FROM UNNEST(
                CASE WHEN ADDRESSES IS NOT NULL THEN JSON_QUERY_ARRAY(ADDRESSES) ELSE [] END
            ) AS addresses_json
            ORDER BY
                CASE
                    WHEN JSON_VALUE(addresses_json, '$.primary') = 'true' THEN 1
                    WHEN JSON_VALUE(addresses_json, '$.primary') IS NULL THEN 2
                    ELSE 3
                END
            LIMIT 1
        ) AS user_country
        , LOWER(TRIM(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.gender'))) AS gender
        , ROW_NUMBER() OVER (PARTITION BY USER_KEY, RECORD_SOURCE ORDER BY LOAD_DATE DESC) AS ROW_NUM
    FROM {{ ref('sat_account_management_users') }}
    WHERE RECORD_SOURCE = 'ACCOUNTMANAGEMENT.hexa.CO.NZ'
    QUALIFY ROW_NUM = 1

)

, prepare_payload AS (

    SELECT

        CURRENT_TIMESTAMP() AS UPDATED_AT
        , ROW_NUM
        , MARKETING_ID AS EXTERNAL_ID
        , CASE WHEN ROW_NUM = 1 THEN
       ( SELECT JSON_STRIP_NULLS(
            TO_JSON(
                STRUCT(
                    IF(
                        JSON_VALUE(USER_CUSTOM_EXTENSION, '$.consentReference') IS NOT NULL,
--                      commented out until CEM API is aligned with this change
--                         STRUCT(
--                             SAFE_CAST(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.consentReference') AS BOOLEAN) AS hexa_digital
--                         ),
                        NULL,
                        NULL
                    ) AS marketing_consent
                {# (
                        SELECT
                            JSON_VALUE(email_json, '$.value')
                        FROM UNNEST(
                            CASE WHEN EMAILS IS NOT NULL THEN JSON_QUERY_ARRAY(EMAILS) ELSE [] END
                        ) AS email_json
                        ORDER BY
                            CASE
                                WHEN JSON_VALUE(email_json, '$.primary') = 'true' THEN 1
                                WHEN JSON_VALUE(email_json, '$.primary') IS NULL THEN 2
                                ELSE 3
                            END
                        LIMIT 1) AS email #}
                , STRUCT(
                    JSON_VALUE(NAME, '$.givenName') AS first_name
                    , JSON_VALUE(NAME, '$.familyName') AS last_name
                    , (
                        SELECT
                            JSON_VALUE(email_json, '$.value')
                        FROM UNNEST(
                            CASE WHEN EMAILS IS NOT NULL THEN JSON_QUERY_ARRAY(EMAILS) ELSE [] END
                        ) AS email_json
                        ORDER BY
                            CASE
                                WHEN JSON_VALUE(email_json, '$.primary') = 'true' THEN 1
                                WHEN JSON_VALUE(email_json, '$.primary') IS NULL THEN 2
                                ELSE 3
                            END
                        LIMIT 1
                    ) AS email
                    , SAFE_CAST(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.emailVerified') AS BOOLEAN) AS email_verified
                    {# , IF(
                            JSON_VALUE(USER_CUSTOM_EXTENSION, '$.emailVerifiedDate') IS NOT NULL,
                            STRUCT(
                                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',
                                    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
                                        JSON_VALUE(USER_CUSTOM_EXTENSION, '$.emailVerifiedDate')), 'UTC'
                                ) AS `$time`
                            ),
                            NULL
                    ) AS email_verified_date #}
                    , (
                        SELECT
                            JSON_VALUE(phonenumbers_json, '$.value')
                        FROM UNNEST(
                            CASE WHEN PHONENUMBERS IS NOT NULL THEN JSON_QUERY_ARRAY(PHONENUMBERS) ELSE [] END
                        ) AS phonenumbers_json
                        ORDER BY
                            CASE
                                WHEN JSON_VALUE(phonenumbers_json, '$.primary') = 'true' THEN 1
                                WHEN JSON_VALUE(phonenumbers_json, '$.primary') IS NULL THEN 2
                                ELSE 3
                            END
                        LIMIT 1
                    ) AS phone_number
                     , CASE
                            WHEN gender IN ('m', 'male', 'man') THEN 'M'
                            WHEN gender IN ('f', 'female', 'woman') THEN 'F'
                            WHEN gender IN ('o', 'non-binary', 'takatapui') THEN 'O'
                            WHEN gender IN ('n', '-1') THEN 'N'
                            WHEN gender IN ('p', 'rather not say', 'rather-not-say') THEN 'P'
                            WHEN gender IS NULL OR gender = 'U' THEN NULL
                            ELSE NULL
                        END AS gender
                    , IF(
                            JSON_VALUE(USER_CUSTOM_EXTENSION, '$.dateOfBirth') IS NOT NULL,
                            STRUCT(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.dateOfBirth') AS `$time`),
                            NULL
                    ) AS date_of_birth
                    , IF(
                            JSON_VALUE(USER_CUSTOM_EXTENSION, '$.yearOfBirth') IS NOT NULL,
                            STRUCT(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.yearOfBirth') AS `$time`),
                            NULL
                    ) AS year_of_birth
                    , (
                        SELECT
                            JSON_VALUE(addresses_json, '$.postalCode')
                        FROM UNNEST(
                            CASE WHEN ADDRESSES IS NOT NULL THEN JSON_QUERY_ARRAY(ADDRESSES) ELSE [] END
                        ) AS addresses_json
                        ORDER BY
                            CASE
                                WHEN JSON_VALUE(addresses_json, '$.primary') = 'true' THEN 1
                                WHEN JSON_VALUE(addresses_json, '$.primary') IS NULL THEN 2
                                ELSE 3
                            END
                        LIMIT 1
                    ) AS postal_code
                    , COALESCE(c.alpha_2_code, u.user_country) AS country
                    , USERNAME AS user_name
                    , DISPLAYNAME as display_name
                    , JSON_VALUE(USER_CUSTOM_EXTENSION, '$.publication') AS source_application
                    , IF(
                            JSON_VALUE(META, '$.created') IS NOT NULL,
                            STRUCT(
                                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',
                                    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
                                        JSON_VALUE(META, '$.created')), 'UTC'
                                ) AS `$time`
                            ),
                            NULL
                    ) AS user_created_date
                    , IF(
                            JSON_VALUE(META, '$.lastModified') IS NOT NULL,
                            STRUCT(
                                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez',
                                    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',
                                        JSON_VALUE(META, '$.lastModified')), 'UTC'
                                ) AS `$time`
                            ),
                            NULL
                    ) AS user_last_modified_date
--                      commented out until CEM API is aligned with this change
                    --, SAFE_CAST(ACTIVE AS BOOLEAN) AS active
                    ) AS digital_account_information
                )
            )
        ))
        ELSE NULL END AS PAYLOAD

    FROM users u
    LEFT JOIN country_codes c ON ((u.user_country = c.country_name) OR (u.user_country = LOWER(c.alpha_2_code)))
    ORDER BY 1 ASC

)

, insert_payload AS (

    SELECT
        UPDATED_AT
        , EXTERNAL_ID
        , PAYLOAD AS PAYLOAD
    FROM prepare_payload
    WHERE PAYLOAD IS NOT NULL

)

SELECT * FROM insert_payload
