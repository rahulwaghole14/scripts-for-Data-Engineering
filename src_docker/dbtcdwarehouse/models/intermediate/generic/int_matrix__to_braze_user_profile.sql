{{ config(
    materialized='incremental',
    unique_key='subs_id',
    tags=['bq_matrix'],
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
-- Retrieve existing hash_keys from the target table for incremental runs
    WITH existing_hash AS (
        SELECT
            subs_id,
            hash_key AS existing_hash_key
        FROM {{ this }} -- points to the current model, which is the target table
    )

{% else %}
-- On the first run, we define existing_hash as an empty table with the right structure
WITH existing_hash AS (
    SELECT
        CAST(NULL AS STRING) AS subs_id,
        CAST(NULL AS STRING) AS existing_hash_key
    FROM UNNEST([]) -- Creates an empty table
)

{% endif %}

-- Alias for the source data is defined outside the conditional to be available in both scenarios
, source_data AS (
    SELECT *
    FROM {{ ref('int_matrix__customers') }}
)
, idm_users AS (
    SELECT
        user_id AS hexa_account_id,
        record_load_dts_utc,
        subscriber_id AS matrix__subscriber_subs_id,
        LOWER(TRIM(email)) AS cleaned_email,
        UPPER(first_name) AS first_name,
        UPPER(last_name) AS last_name,
        CASE WHEN phone_type = 'work' THEN contact_phone ELSE NULL END AS work_phone,
        contact_phone,
        country
    FROM {{ ref('stg_idm__user_profiles') }}
    WHERE subscriber_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY subscriber_id
        ORDER BY record_load_dts_utc DESC
    ) = 1
)

, matrix_users AS (
    SELECT
        hash_key,
        subs_id,
        LOWER(TRIM(PersContact3)) AS cleaned_email
    FROM source_data
)

-- Combine IDM and Matrix users, identify matching emails
, combined_emails AS (
    SELECT
        matrix_users.hash_key,
        matrix_users.subs_id,
        idm_users.hexa_account_id,
        idm_users.cleaned_email AS hexa_account_email,
        matrix_users.cleaned_email AS matrix_email,
        CASE 
            WHEN idm_users.cleaned_email = matrix_users.cleaned_email THEN 1 
            ELSE 0 
        END AS is_email_same,
        CASE 
            WHEN matrix__subscriber_subs_id is null THEN 0
            ELSE 1 
        END AS is_linked_account,
        idm_users.first_name,
        idm_users.last_name,
        idm_users.work_phone,
        idm_users.contact_phone,
        idm_users.country
    FROM matrix_users 
    LEFT JOIN idm_users 
        ON idm_users.matrix__subscriber_subs_id = matrix_users.subs_id
)

, combined_user_profile AS (
    SELECT 
        source_data.hash_key,
        source_data.subs_id,
        CASE
            WHEN combined_emails.is_email_same = 1 THEN combined_emails.hexa_account_email
            WHEN combined_emails.is_email_same = 0 AND combined_emails.hexa_account_email IS NOT NULL THEN combined_emails.hexa_account_email 
        END AS email,
        CASE WHEN combined_emails.is_email_same = 1 THEN subtype_id ELSE NULL END AS subtype_id,
        CASE WHEN combined_emails.is_email_same = 1 THEN title ELSE NULL END AS title,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN FirstName 
            ELSE combined_emails.first_name 
        END AS FirstName,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN SurName 
            ELSE combined_emails.last_name 
        END AS SurName,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN matrix_email 
            ELSE combined_emails.hexa_account_email 
        END AS PersContact3,
        CASE WHEN combined_emails.is_email_same = 1 THEN HOME ELSE NULL END AS HOME,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN WORK 
            ELSE combined_emails.work_phone 
        END AS WORK,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN MOBILE 
            ELSE combined_emails.contact_phone 
        END AS MOBILE,
        CASE WHEN combined_emails.is_email_same = 1 THEN DoNotCall ELSE NULL END AS DoNotCall,
        CASE WHEN combined_emails.is_email_same = 1 THEN AddrLine3 ELSE NULL END AS AddrLine3,
        CASE WHEN combined_emails.is_email_same = 1 THEN AddrLine4 ELSE NULL END AS AddrLine4,
        CASE WHEN combined_emails.is_email_same = 1 THEN AddrLine5 ELSE NULL END AS AddrLine5,
        CASE WHEN combined_emails.is_email_same = 1 THEN AddrLine6 ELSE NULL END AS AddrLine6,
        CASE 
            WHEN combined_emails.is_email_same = 1 THEN country_name 
            ELSE combined_emails.country 
        END AS country_name,
        1 AS has_matrix_or_idm_attributes
    FROM source_data
    LEFT JOIN combined_emails 
        ON source_data.hash_key = combined_emails.hash_key
    WHERE combined_emails.is_email_same = 1 AND  is_linked_account =1
    OR (combined_emails.is_email_same = 0 AND combined_emails.hexa_account_email IS NOT NULL)

    UNION ALL

    SELECT 
        source_data.hash_key,
        source_data.subs_id,
        combined_emails.matrix_email AS email,
        NULL AS subtype_id,
        NULL AS title,
        NULL AS FirstName,
        NULL AS SurName,
        NULL AS PersContact3,
        NULL AS HOME,
        NULL AS WORK,
        NULL AS MOBILE,
        NULL AS DoNotCall,
        NULL AS AddrLine3,
        NULL AS AddrLine4,
        NULL AS AddrLine5,
        NULL AS AddrLine6,
        NULL AS country_name,
        0 AS has_matrix_or_idm_attributes

    FROM source_data
    LEFT JOIN combined_emails 
        ON source_data.hash_key = combined_emails.hash_key 
    WHERE combined_emails.is_email_same = 0  
        AND  is_linked_account =1
        AND combined_emails.hexa_account_email IS NULL

    UNION ALL
    
    SELECT 
        source_data.hash_key,
        source_data.subs_id,
        combined_emails.matrix_email  AS email,
        subtype_id,
        title ,
        FirstName,
        SurName,
        matrix_email AS PersContact3,
        HOME,
        WORK,
        MOBILE,
        DoNotCall,
        AddrLine3,
        AddrLine4,
        AddrLine5,
        AddrLine6,
        country_name,
        1 AS has_matrix_or_idm_attributes
    FROM source_data
    LEFT JOIN combined_emails 
        ON source_data.hash_key = combined_emails.hash_key
    WHERE  is_linked_account = 0

)

-- Select records that have a different hash key or are not present in the target table
-- Use print namespace for UUID generation
SELECT
    {{ format_uuid(generate_uuid_v5("combined_user_profile.subs_id", env_var('UUID_NAMESPACE_PRINT'))) }} AS marketing_id,
    combined_user_profile.hash_key,
    combined_user_profile.subs_id,
    combined_user_profile.email,
    combined_user_profile.subtype_id,
    combined_user_profile.title,
    combined_user_profile.FirstName,
    combined_user_profile.SurName,
    combined_user_profile.PersContact3,
    combined_user_profile.HOME,
    combined_user_profile.WORK,
    combined_user_profile.MOBILE,
    combined_user_profile.DoNotCall,
    combined_user_profile.AddrLine3,
    combined_user_profile.AddrLine4,
    combined_user_profile.AddrLine5,
    combined_user_profile.AddrLine6,
    combined_user_profile.country_name,
    has_matrix_or_idm_attributes,
    CURRENT_TIMESTAMP() AS updated_at
FROM combined_user_profile
LEFT JOIN existing_hash USING (subs_id)
WHERE
    combined_user_profile.hash_key != existing_hash.existing_hash_key -- hash key is different
    OR existing_hash.existing_hash_key IS NULL -- new record
