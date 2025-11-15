{{ config(
    tags = ['braze_sync_mart'],
    partition_by={
      "field": "UPDATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH user_profiles AS (

    SELECT 
        id,
        externalId,
        marketing_id,
        name.givenName AS given_name,
        name.familyName AS family_name,
        active,
        user_custom_extension.yearOfBirth AS year_of_birth,
        user_custom_extension.publication AS publication,
        record_loaded_dts
    FROM {{ source('idm', 'account_management__user_profiles') }}
    WHERE active = TRUE
),

fact_account_management_users AS (

    WITH country_codes AS (
        SELECT 
            REGEXP_REPLACE(LOWER(TRIM(country_name)), r'[^a-z ]', '') AS country_name,
            alpha_2_code
        FROM {{ ref('country_codes') }}
    ),

    users AS (
        SELECT *,
            (
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
            ) AS user_country,
            LOWER(TRIM(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.gender'))) AS gender,
            ROW_NUMBER() OVER (PARTITION BY USER_KEY, RECORD_SOURCE ORDER BY LOAD_DATE DESC) AS ROW_NUM
        FROM {{ ref('sat_account_management_users') }}
        WHERE RECORD_SOURCE = 'ACCOUNTMANAGEMENT.hexa.CO.NZ'
        QUALIFY ROW_NUM = 1
    ),

    prepare_payload AS (
        SELECT
            CURRENT_TIMESTAMP() AS UPDATED_AT,
            ROW_NUM,
            MARKETING_ID AS EXTERNAL_ID,
            CASE 
                WHEN ROW_NUM = 1 THEN (
                    SELECT JSON_STRIP_NULLS(
                        TO_JSON(
                            STRUCT(
                                NULL AS marketing_consent,
                                STRUCT(
                                    JSON_VALUE(NAME, '$.givenName') AS first_name,
                                    JSON_VALUE(NAME, '$.familyName') AS last_name,
                                    (
                                        SELECT JSON_VALUE(email_json, '$.value')
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
                                    ) AS email,
                                    SAFE_CAST(JSON_VALUE(USER_CUSTOM_EXTENSION, '$.emailVerified') AS BOOLEAN) AS email_verified,
                                    (
                                        SELECT JSON_VALUE(phonenumbers_json, '$.value')
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
                                    ) AS phone_number,
                                    CASE
                                        WHEN gender IN ('m','male','man') THEN 'M'
                                        WHEN gender IN ('f','female','woman') THEN 'F'
                                        WHEN gender IN ('o','non-binary','takatapui') THEN 'O'
                                        WHEN gender IN ('n','-1') THEN 'N'
                                        WHEN gender IN ('p','rather not say','rather-not-say') THEN 'P'
                                        ELSE NULL
                                    END AS gender,
                                    JSON_VALUE(USER_CUSTOM_EXTENSION, '$.dateOfBirth') AS date_of_birth,
                                    JSON_VALUE(USER_CUSTOM_EXTENSION, '$.yearOfBirth') AS year_of_birth,
                                    (
                                        SELECT JSON_VALUE(addresses_json, '$.postalCode')
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
                                    ) AS postal_code,
                                    COALESCE(c.alpha_2_code, u.user_country) AS country,
                                    USERNAME AS user_name,
                                    DISPLAYNAME AS display_name,
                                    JSON_VALUE(USER_CUSTOM_EXTENSION, '$.publication') AS source_application
                                ) AS digital_account_information
                            )
                        )
                    )
                )
                ELSE NULL 
            END AS PAYLOAD
        FROM users u
        LEFT JOIN country_codes c
            ON (u.user_country = c.country_name OR u.user_country = LOWER(c.alpha_2_code))
        ORDER BY 1 ASC
    ),

    insert_payload AS (
        SELECT
            UPDATED_AT,
            EXTERNAL_ID,
            PAYLOAD
        FROM prepare_payload
        WHERE PAYLOAD IS NOT NULL
    )

    SELECT * FROM insert_payload
),

fact_users AS (
    SELECT 
        EXTERNAL_ID,
        PAYLOAD AS digital_account_information,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.email') AS email,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.country') AS country,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.gender') AS gender,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.postal_code') AS postal_code,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.display_name') AS display_name,
        JSON_VALUE(PAYLOAD, '$.digital_account_information.user_name') AS user_name
    FROM fact_account_management_users
),

combined_data AS (
    SELECT
        CURRENT_TIMESTAMP() AS UPDATED_AT,
        fu.EXTERNAL_ID AS external_id,
        fu.email AS email,
        up.given_name AS first_name,
        up.family_name AS last_name,
        fu.country AS country,
        up.year_of_birth AS dob,
        fu.gender AS gender,
        up.id AS hexa_account_id,
        fu.display_name AS digital_display_name,
        SAFE_CAST(fu.postal_code AS INT64) AS digital_postcode,
        fu.user_name AS digital_username,
        fu.digital_account_information,
        SAFE_CAST(fu.postal_code AS INT64) AS post_code,
        up.publication AS digital__publication,
        up.record_loaded_dts,
        ROW_NUMBER() OVER (
            PARTITION BY fu.EXTERNAL_ID
            ORDER BY up.record_loaded_dts DESC
        ) AS row_num
    FROM user_profiles up
    INNER JOIN fact_users fu 
        ON up.marketing_id = fu.EXTERNAL_ID
    WHERE fu.email IS NOT NULL
        AND LOWER(TRIM(up.publication)) NOT IN ('the-press', 'the-post', 'waikato-times')
),

final_output AS (
    SELECT
        cd.UPDATED_AT,
        TO_JSON(STRUCT(
            cd.external_id,
            cd.email,
            cd.first_name,
            cd.last_name,
            cd.country,
            cd.dob,
            cd.gender,
            cd.hexa_account_id,
            cd.digital_display_name,
            cd.digital_postcode,
            cd.digital_username,
            JSON_QUERY(cd.digital_account_information, '$.digital_account_information') AS digital_account_information,
            cd.post_code AS postal_code,
            cd.digital__publication,
            JSON_VALUE(fu.digital_account_information, '$.digital_account_information.phone_number') AS phone,
            JSON_QUERY(cs.PAYLOAD, '$.communication_subscriptions') AS communication_subscriptions
        )) AS PAYLOAD,
        cd.external_id AS EXTERNAL_ID
    FROM combined_data cd
    LEFT JOIN fact_users fu
        ON cd.external_id = fu.external_id
    LEFT JOIN {{ source('braze', 'customer_communication_subscriptions') }} cs
        ON cd.external_id = cs.EXTERNAL_ID
    WHERE cd.row_num = 1
)

SELECT * FROM final_output
