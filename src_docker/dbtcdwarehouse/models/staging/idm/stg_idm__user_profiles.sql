{{
  config(
        tags=['braze']
    )
}}

WITH groupm_codes AS (

    SELECT POSTCODE
        , LEADING_LIFESTYLES
        , METROTECH
        , ASPIRATIONALS
        , HEARTH_HOME
        , DOING_FINE
        , FAIR_GO
        , URBAN_ACCESSIBILITY
        , TRADES
        , LARGE_SCALE_HOMEOWNERS
        , AFFLUENT_RURAL
    FROM {{ ref('stg_groupm__postcodes') }}
)

, groupm_codes_new AS (

    SELECT * FROM {{ ref('stg_groupm__postcodes_new') }}

)

, source AS (

    SELECT
        account_management__user_profiles.*
        , account_management__user_profiles.name AS nameorig
        , un_addresses
        , un_emails
        , un_phoneNumbers
        , un_newsletter_subs
        , CASE
            WHEN user_custom_extension.gender IN ('M', 'Male', 'Man') THEN 'M'
            WHEN user_custom_extension.gender IN ('F', 'Female', 'Woman') THEN 'F'
            WHEN user_custom_extension.gender IN ('O', 'Non-binary', 'Takatapui') THEN 'O'
            WHEN user_custom_extension.gender IN ('N', '-1') THEN 'N'
            WHEN user_custom_extension.gender IN ('P', 'Rather not say', 'Rather-Not-Say') THEN 'P'
            WHEN user_custom_extension.gender IS NULL OR user_custom_extension.gender = 'U' THEN NULL
            ELSE NULL
        END AS gender_groups
        , CASE
            WHEN COALESCE(user_custom_extension.dateOfBirth, user_custom_extension.yearOfBirth) IS NULL THEN NULL
            ELSE CAST(FLOOR(DATE_DIFF(CURRENT_DATE(), COALESCE(user_custom_extension.dateOfBirth, user_custom_extension.yearOfBirth), YEAR)) AS STRING)
        END AS age
        , groupm_codes.LEADING_LIFESTYLES as LEADING_LIFESTYLES
        , groupm_codes.METROTECH as METROTECH
        , groupm_codes.ASPIRATIONALS as ASPIRATIONALS
        , groupm_codes.HEARTH_HOME as HEARTH_HOME
        , groupm_codes.DOING_FINE as DOING_FINE
        , groupm_codes.FAIR_GO as FAIR_GO
        , groupm_codes.URBAN_ACCESSIBILITY as URBAN_ACCESSIBILITY
        , groupm_codes.TRADES as TRADES
        , groupm_codes.LARGE_SCALE_HOMEOWNERS as LARGE_SCALE_HOMEOWNERS
        , groupm_codes.AFFLUENT_RURAL as AFFLUENT_RURAL
        , groupm_codes_new.*
    FROM {{ source('idm', 'account_management__user_profiles') }} account_management__user_profiles
    LEFT JOIN UNNEST(addresses) AS un_addresses
    LEFT JOIN UNNEST(emails) AS un_emails
    LEFT JOIN UNNEST(phoneNumbers) AS un_phoneNumbers
    LEFT JOIN UNNEST(user_custom_extension.newsletterSubscription) AS un_newsletter_subs
    LEFT JOIN groupm_codes ON SAFE_CAST(un_addresses.postalCode AS INT) = groupm_codes.POSTCODE
    LEFT JOIN groupm_codes_new ON SAFE_CAST(un_addresses.postalCode AS INT) = groupm_codes_new.POSTCODE

)

, renamed AS (

    SELECT
        id AS user_id
        , ANY_VALUE(source.ppid) AS ppid
        , ANY_VALUE(source.externalId) AS adobe_id
        , CAST(ANY_VALUE(user_custom_extension.subscriberId) AS STRING) AS subscriber_id
        , ANY_VALUE(source.userName) AS username
        , "IDM" AS record_source
        , CASE
            WHEN ANY_VALUE(source.active) IS TRUE THEN 'True'
            WHEN ANY_VALUE(source.active) IS FALSE THEN 'False'
            ELSE NULL
        END AS active
        , ANY_VALUE(un_addresses.country) AS country
        , ANY_VALUE(un_addresses.postalCode) AS postcode
        , ANY_VALUE(un_addresses.street_address) AS street_address -- not used in braze
        , ANY_VALUE(source.displayName) AS display_name
        , CASE
            WHEN ANY_VALUE(un_emails.primary) IS TRUE THEN 'True'
            WHEN ANY_VALUE(un_emails.primary) IS FALSE THEN 'False'
            ELSE NULL
        END AS emails_primary -- not used in braze
        , ANY_VALUE(un_emails.type) AS emails_type
        , ANY_VALUE(un_emails.value) AS email
        , ANY_VALUE(meta.resourceType) AS resource_type
        , ANY_VALUE(nameorig.familyName) AS last_name
        , ANY_VALUE(nameorig.givenName) AS first_name
        , ANY_VALUE(un_phoneNumbers.type) AS phone_type
        , ANY_VALUE(un_phoneNumbers.value) AS contact_phone
        , CAST(NULL AS STRING) AS timezone
        , CASE
            WHEN ANY_VALUE(user_custom_extension.consentReference) IS TRUE THEN 'True'
            WHEN ANY_VALUE(user_custom_extension.consentReference) IS FALSE THEN 'False'
            ELSE NULL
        END AS user_consent
        , CASE
            WHEN ANY_VALUE(user_custom_extension.emailVerified) IS TRUE THEN 'True'
            WHEN ANY_VALUE(user_custom_extension.emailVerified) IS FALSE THEN 'False'
            ELSE NULL
        END AS email_verified -- not used in braze
        , ANY_VALUE(user_custom_extension.gender) AS gender
        , CASE
            WHEN ANY_VALUE(user_custom_extension.mobileNumberVerified) IS TRUE THEN 'True'
            WHEN ANY_VALUE(user_custom_extension.mobileNumberVerified) IS FALSE THEN 'False'
            ELSE NULL
        END AS mobile_verified
        , CASE
            WHEN CAST(ANY_VALUE(user_custom_extension.dateOfBirth) AS STRING) IS NULL THEN 'NaT'
            ELSE CAST(ANY_VALUE(user_custom_extension.dateOfBirth) AS STRING)
        END AS date_of_birth
        , CAST(ANY_VALUE(meta.created) AS STRING) AS created_date -- not used in braze
        , CAST(ANY_VALUE(meta.lastModified) AS STRING) AS last_modified -- not used in braze
        , CAST(ANY_VALUE(user_custom_extension.emailVerifiedDate) AS STRING) AS verified_date -- not used in braze
        , 'None' AS mobile_verified_date -- not used in braze, need to add this in ELT workflow
        , CASE
            WHEN CAST(ANY_VALUE(user_custom_extension.yearOfBirth) AS STRING) IS NULL THEN 'NaT'
            ELSE CAST(ANY_VALUE(user_custom_extension.yearOfBirth) AS STRING)
        END AS year_of_birth
        , CAST(NULL AS STRING) AS city_region
        , CAST(NULL AS STRING) AS locality
        , CAST(NULL AS STRING) AS hash_diff
        , CASE
            WHEN
                ARRAY_LENGTH(ARRAY_AGG(un_newsletter_subs)) = 0
                OR ARRAY_AGG(un_newsletter_subs) IS NULL THEN NULL
            WHEN CAST(TO_JSON_STRING(ARRAY_AGG(un_newsletter_subs)) AS STRING) = '[null]' THEN NULL
            ELSE CAST(TO_JSON_STRING(ARRAY_AGG(un_newsletter_subs)) AS STRING)
        END AS newsletter_subs
        , SAFE_CAST(ANY_VALUE(source.age) AS INT64) AS age
        , ANY_VALUE(source.gender_groups) AS gender_groups
        , ANY_VALUE(source.LEADING_LIFESTYLES) AS leading_lifestyles
        , ANY_VALUE(source.METROTECH) AS metrotech
        , ANY_VALUE(source.ASPIRATIONALS) AS aspirational
        , ANY_VALUE(source.HEARTH_HOME) AS hearth_home
        , ANY_VALUE(source.DOING_FINE) AS doing_fine
        , ANY_VALUE(source.FAIR_GO) AS fair_go
        , ANY_VALUE(source.URBAN_ACCESSIBILITY) AS urban_accessibility
        , ANY_VALUE(source.TRADES) AS trades
        , ANY_VALUE(source.LARGE_SCALE_HOMEOWNERS) AS large_scale_homeowners
        , ANY_VALUE(source.AFFLUENT_RURAL) AS affluent_rural
        , ANY_VALUE(source.GROUPM_BLUECHIP) AS GROUPM_BLUECHIP
        , ANY_VALUE(source.GROUPM_SMARTMONEY) AS GROUPM_SMARTMONEY
        , ANY_VALUE(source.GROUPM_FINANCIALFREEDOM) AS GROUPM_FINANCIALFREEDOM
        , ANY_VALUE(source.GROUPM_SELFMADLIFESTYLERS) AS GROUPM_SELFMADLIFESTYLERS
        , ANY_VALUE(source.GROUPM_WORLDLYANDWISE) AS GROUPM_WORLDLYANDWISE
        , ANY_VALUE(source.GROUPM_STATUSMATTERS) AS GROUPM_STATUSMATTERS
        , ANY_VALUE(source.GROUPM_SAVVYSELFSTARTERS) AS GROUPM_SAVVYSELFSTARTERS
        , ANY_VALUE(source.GROUPM_HUMANITARIANS) AS GROUPM_HUMANITARIANS
        , ANY_VALUE(source.GROUPM_FULLHOUSE) AS GROUPM_FULLHOUSE
        , ANY_VALUE(source.GROUPM_YOUNGANDPLATINUM) AS GROUPM_YOUNGANDPLATINUM
        , ANY_VALUE(source.GROUPM_HEALTHYWEALTHYANDWISE) AS GROUPM_HEALTHYWEALTHYANDWISE
        , ANY_VALUE(source.GROUPM_CULTURALPIONEERS) AS GROUPM_CULTURALPIONEERS
        , ANY_VALUE(source.GROUPM_NEWSCHOOLCOOL) AS GROUPM_NEWSCHOOLCOOL
        , ANY_VALUE(source.GROUPM_URBANENTERTAINERS) AS GROUPM_URBANENTERTAINERS
        , ANY_VALUE(source.GROUPM_NEWKIWIS) AS GROUPM_NEWKIWIS
        , ANY_VALUE(source.GROUPM_SOCIALACADEMICS) AS GROUPM_SOCIALACADEMICS
        , ANY_VALUE(source.GROUPM_BIGFUTURE) AS GROUPM_BIGFUTURE
        , ANY_VALUE(source.GROUPM_FITANDFAB) AS GROUPM_FITANDFAB
        , ANY_VALUE(source.GROUPM_QUIETACHIEVER) AS GROUPM_QUIETACHIEVER
        , ANY_VALUE(source.GROUPM_CAREERZONE) AS GROUPM_CAREERZONE
        , ANY_VALUE(source.GROUPM_ONTHEIRWAY) AS GROUPM_ONTHEIRWAY
        , ANY_VALUE(source.GROUPM_GETTINGAHEAD) AS GROUPM_GETTINGAHEAD
        , ANY_VALUE(source.GROUPM_DOMESTICJUGGLERS) AS GROUPM_DOMESTICJUGGLERS
        , ANY_VALUE(source.GROUPM_AVERAGEKIWI) AS GROUPM_AVERAGEKIWI
        , ANY_VALUE(source.GROUPM_DOWNTHEARTH) AS GROUPM_DOWNTHEARTH
        , ANY_VALUE(source.GROUPM_DOMESTICBLISS) AS GROUPM_DOMESTICBLISS
        , ANY_VALUE(source.GROUPM_DONEGOOD) AS GROUPM_DONEGOOD
        , ANY_VALUE(source.GROUPM_TRADITIONALVALUES) AS GROUPM_TRADITIONALVALUES
        , ANY_VALUE(source.GROUPM_CASTLEANDKIDS) AS GROUPM_CASTLEANDKIDS
        , ANY_VALUE(source.GROUPM_HOUSEPROUD) AS GROUPM_HOUSEPROUD
        , ANY_VALUE(source.GROUPM_FAMILYFIRST) AS GROUPM_FAMILYFIRST
        , ANY_VALUE(source.GROUPM_RELAXEDLIVING) AS GROUPM_RELAXEDLIVING
        , ANY_VALUE(source.GROUPM_CAUTIOUSCONSERVATIVES) AS GROUPM_CAUTIOUSCONSERVATIVES
        , ANY_VALUE(source.GROUPM_MAKINGTHERENT) AS GROUPM_MAKINGTHERENT
        , ANY_VALUE(source.GROUPM_MAKINGENDSMEET) AS GROUPM_MAKINGENDSMEET
        , ANY_VALUE(source.GROUPM_DOINGITTOUGH) AS GROUPM_DOINGITTOUGH
        , ANY_VALUE(source.GROUPM_LIFESTRIVERS) AS GROUPM_LIFESTRIVERS
        , ANY_VALUE(source.GROUPM_ACTIVESOCIAL) AS GROUPM_ACTIVESOCIAL
        , ANY_VALUE(source.GROUPM_HOMEENTERTAINMENT) AS GROUPM_HOMEENTERTAINMENT
        , ANY_VALUE(source.GROUPM_BETTERDAYS) AS GROUPM_BETTERDAYS
        , ANY_VALUE(source.GROUPM_FAITHANDFAMILY) AS GROUPM_FAITHANDFAMILY
        , ANY_VALUE(source.GROUPM_SIMPLELIVING) AS GROUPM_SIMPLELIVING
        , ANY_VALUE(source.GROUPM_QUIETHOMELIFE) AS GROUPM_QUIETHOMELIFE
        , ANY_VALUE(source.GROUPM_AREASINTRANSITION) AS GROUPM_AREASINTRANSITION
        , ANY_VALUE(source.GROUPM_STILLWORKING) AS GROUPM_STILLWORKING
        , ANY_VALUE(source.GROUPM_NEWBEGINNINGS) AS GROUPM_NEWBEGINNINGS
        , ANY_VALUE(source.GROUPM_STRUGGLESTREET) AS GROUPM_STRUGGLESTREET
        , ANY_VALUE(source.GROUPM_COUPONCLIPPERS) AS GROUPM_COUPONCLIPPERS
        , ANY_VALUE(source.GROUPM_PENNYWISE) AS GROUPM_PENNYWISE
        , ANY_VALUE(source.GROUPM_REALWORKING) AS GROUPM_REALWORKING
        , ANY_VALUE(source.GROUPM_BUDGETLIVING) AS GROUPM_BUDGETLIVING
        , ANY_VALUE(CAST(source.record_loaded_dts AS STRING)) AS record_load_dts_utc -- not used in braze
        , ANY_VALUE(LOWER(source.marketing_id)) AS marketing_id
        , ANY_VALUE(LOWER(source.marketing_id_email)) AS marketing_id_email
        , ANY_VALUE(CAST(source.record_loaded_dts AS TIMESTAMP)) AS updated_at
FROM source GROUP BY id

)

SELECT
    *
    , TO_BASE64(MD5(TO_JSON_STRING(STRUCT(
        user_id
        , ppid
        , adobe_id
        , subscriber_id
        , username
        , record_source
        , active
        , country
        , postcode
        , street_address
        , display_name
        , emails_primary
        , emails_type
        , email
        , resource_type
        , last_name
        , first_name
        , phone_type
        , contact_phone
        , user_consent
        , email_verified
        , gender
        , mobile_verified
        , date_of_birth
        , created_date
        , last_modified
        , verified_date
        , mobile_verified_date
        , year_of_birth
        , city_region
        , locality
        , hash_diff
        , newsletter_subs
        , age
        , gender_groups
        , leading_lifestyles
        , metrotech
        , aspirational
        , hearth_home
        , doing_fine
        , fair_go
        , urban_accessibility
        , trades
        , large_scale_homeowners
        , affluent_rural
        , GROUPM_BLUECHIP
        , GROUPM_SMARTMONEY
        , GROUPM_FINANCIALFREEDOM
        , GROUPM_SELFMADLIFESTYLERS
        , GROUPM_WORLDLYANDWISE
        , GROUPM_STATUSMATTERS
        , GROUPM_SAVVYSELFSTARTERS
        , GROUPM_HUMANITARIANS
        , GROUPM_FULLHOUSE
        , GROUPM_YOUNGANDPLATINUM
        , GROUPM_HEALTHYWEALTHYANDWISE
        , GROUPM_CULTURALPIONEERS
        , GROUPM_NEWSCHOOLCOOL
        , GROUPM_URBANENTERTAINERS
        , GROUPM_NEWKIWIS
        , GROUPM_SOCIALACADEMICS
        , GROUPM_BIGFUTURE
        , GROUPM_FITANDFAB
        , GROUPM_QUIETACHIEVER
        , GROUPM_CAREERZONE
        , GROUPM_ONTHEIRWAY
        , GROUPM_GETTINGAHEAD
        , GROUPM_DOMESTICJUGGLERS
        , GROUPM_AVERAGEKIWI
        , GROUPM_DOWNTHEARTH
        , GROUPM_DOMESTICBLISS
        , GROUPM_DONEGOOD
        , GROUPM_TRADITIONALVALUES
        , GROUPM_CASTLEANDKIDS
        , GROUPM_HOUSEPROUD
        , GROUPM_FAMILYFIRST
        , GROUPM_RELAXEDLIVING
        , GROUPM_CAUTIOUSCONSERVATIVES
        , GROUPM_MAKINGTHERENT
        , GROUPM_MAKINGENDSMEET
        , GROUPM_DOINGITTOUGH
        , GROUPM_LIFESTRIVERS
        , GROUPM_ACTIVESOCIAL
        , GROUPM_HOMEENTERTAINMENT
        , GROUPM_BETTERDAYS
        , GROUPM_FAITHANDFAMILY
        , GROUPM_SIMPLELIVING
        , GROUPM_QUIETHOMELIFE
        , GROUPM_AREASINTRANSITION
        , GROUPM_STILLWORKING
        , GROUPM_NEWBEGINNINGS
        , GROUPM_STRUGGLESTREET
        , GROUPM_COUPONCLIPPERS
        , GROUPM_PENNYWISE
        , GROUPM_REALWORKING
        , GROUPM_BUDGETLIVING
        , record_load_dts_utc
        , marketing_id
        , marketing_id_email
        , updated_at
    )))) AS row_hash
FROM renamed
