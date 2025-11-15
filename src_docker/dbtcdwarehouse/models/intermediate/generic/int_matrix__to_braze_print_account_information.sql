{{ config(
    tags='bq_matrix',
    partition_by={
      "field": "UPDATED_AT",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH matrix_data AS (
    SELECT * FROM {{ ref('int_matrix__to_braze_user_profiles_final') }}
    )

, prepare_payload AS (
    SELECT
        updated_at AS UPDATED_AT
        , MARKETING_ID AS EXTERNAL_ID
        , JSON_STRIP_NULLS(
            TO_JSON(
                STRUCT(
                    first_name,
                    last_name,
                    email,
                    STRUCT(
                        first_name,
                        last_name,
                        title,
                        email as email_address,
                        mobile as phone_mobile,
                        home as phone_home,
                        work as phone_work,
                        print__postcode as postcode,
                        print__sub_id as sub_id,
                        print__sub_type as sub_type,
                        CAST(has_idm_am_account AS INT64) = 1 AS has_idm_am_account,
                        CAST(print__do_not_contact AS INT64) = 1 AS do_not_contact,
                        CAST(is_digital_masthead_subscriber AS INT64) = 1 AS is_digital_masthead_subscriber,
                        hexa_account_id
                    ) AS print_account_information
                )
            )
        ) AS PAYLOAD

    FROM matrix_data
    ORDER BY 1 ASC
)

SELECT
    UPDATED_AT
    , EXTERNAL_ID
    , PAYLOAD AS PAYLOAD
FROM prepare_payload
WHERE PAYLOAD IS NOT NULL

