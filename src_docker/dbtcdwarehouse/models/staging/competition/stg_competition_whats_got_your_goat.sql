WITH source AS (

    SELECT * FROM {{ source('competition', 'competition_whats_got_your_goat_Form_Responses_1') }}

)

, renamed AS (

    SELECT
    {{ adapter.quote("What_is_your_phone_number_") }} AS phone_number -- noqa
    , {{ adapter.quote("Where_in_New_Zealand_do_you_live_") }} AS geo_location -- noqa
    , {{ adapter.quote("What_is_your_name_") }} AS name -- noqa
    , {{ adapter.quote("Confirmation") }} AS confirmation -- noqa
    , {{ adapter.quote("Timestamp") }} AS timestamp -- noqa
    , {{ adapter.quote("Tell_us_about_the_issue_that_s_on_your_mind") }} AS issue -- noqa
    , LOWER(TRIM({{ adapter.quote("Email_Address") }})) AS email -- noqa
    FROM source
    {# -- confirmation contains string "I agree to be contacted regarding this issue" #}
    {# WHERE confirmation LIKE '%I agree to be contacted regarding this issue%' #}

)

, marketing_id AS (

    SELECT
        *
        , {{ format_uuid(generate_uuid_v5("email")) }} AS marketing_id  -- noqa
    FROM renamed

)

SELECT * FROM marketing_id
