{{
  config(
    tags = ['vlt_stage_user']
    )
}}

with source as (

      select * from {{ source('idm', 'account_management__user_profiles') }}

)

, renamed as (

    select

        {{ adapter.quote("id") }} AS USER_KEY
        , TO_JSON_STRING({{ adapter.quote("schemas") }}) AS SCHEMAS
        , {{ adapter.quote("externalId") }} AS EXTERNALID
        , {{ adapter.quote("userName") }} AS USERNAME
        , {{ adapter.quote("displayName") }} AS DISPLAYNAME
        , TO_JSON_STRING({{ adapter.quote("meta") }}) AS META
        , TO_JSON_STRING({{ adapter.quote("name") }}) AS NAME
        , {{ adapter.quote("active") }} AS ACTIVE
        -- Convert the repeated emails field to a JSON string
        , TO_JSON_STRING({{ adapter.quote("emails") }}) AS EMAILS
        -- Convert the repeated ADDRESSES field to a JSON string
        , TO_JSON_STRING({{ adapter.quote("addresses") }}) AS ADDRESSES
        -- Convert the repeated phoneNumbers field to a JSON string
        , TO_JSON_STRING({{ adapter.quote("phoneNumbers") }}) AS PHONENUMBERS
        , TO_JSON_STRING({{ adapter.quote("roles") }}) AS ROLES
        , TO_JSON_STRING({{ adapter.quote("user_custom_extension") }}) AS USER_CUSTOM_EXTENSION
        , {{ adapter.quote("marketing_id") }} AS MARKETING_ID
        , {{ adapter.quote("marketing_id_email") }} AS MARKETING_ID_EMAIL
        , {{ adapter.quote("record_loaded_dts") }} AS RECORD_LOADED_DTS
        , {{ adapter.quote("timezone") }} AS TIMEZONE
        , {{ adapter.quote("ppid") }} AS PPID
        , `meta`.created AS RECORD_CREATED_DTS
        , `meta`.lastModified AS RECORD_LAST_MODIFIED_DTS

    from source

)

select *
    , COALESCE(RECORD_LOADED_DTS, RECORD_LAST_MODIFIED_DTS, RECORD_CREATED_DTS, CURRENT_TIMESTAMP()) AS LOAD_DTS
from renamed
