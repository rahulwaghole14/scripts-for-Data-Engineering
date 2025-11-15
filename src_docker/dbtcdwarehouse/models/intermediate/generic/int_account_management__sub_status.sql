{{
  config(
        tags=['braze']
    )
}}

{# set braze_sub_group_id for general marketing hexa.co.nz_marketing #}
{% set braze_sub_group_id = 'acaa104a-65ad-47e1-84e7-3ed6359cce60' %}

WITH source AS (

    SELECT
        LOWER(marketing_id) AS external_id
    FROM {{ ref('stg_idm__user_profiles') }}
    WHERE newsletter_subs IS NOT NULL

)

SELECT
    external_id
    , '{{ braze_sub_group_id }}' AS braze_sub_group_id
    {# row_hash for data-diff #}
    , TO_BASE64(MD5(TO_JSON_STRING(STRUCT(
        external_id
        , '{{ braze_sub_group_id }}'
    )))) AS row_hash
FROM source
