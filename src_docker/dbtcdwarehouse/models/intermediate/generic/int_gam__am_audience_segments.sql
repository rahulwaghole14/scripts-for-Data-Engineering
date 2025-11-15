{{
  config(
    materialized = 'table',
    tags=['gam']
    )
}}

WITH account_management__user_profiles AS (

    SELECT ppid AS PPID
        , CASE WHEN age >= 18 AND age <= 24 THEN '18_24'
            WHEN age >= 25 AND age <= 34 THEN '25_34'
            WHEN age >= 35 AND age <= 44 THEN '35_44'
            WHEN age >= 45 AND age <= 54 THEN '45_54'
            WHEN age >= 55 AND age <= 64 THEN '55_64'
            WHEN age >= 65 THEN '65_'
            ELSE NULL
        END AS age_groups_primary
        , CASE WHEN age >= 18 AND age <= 40 THEN '18_40'
            WHEN age >= 30 AND age <= 54 THEN '30_54'
            WHEN age >= 50 THEN '50_'
            ELSE NULL
        END AS age_groups_secondary
        , CASE WHEN gender_groups = 'F' THEN 'F' -- Females
          WHEN gender_groups = 'M' THEN 'M' -- Males
          WHEN gender_groups = 'O' THEN 'O' -- Non-Binary
          ELSE 'U' -- Unknown
        END as gender_groups_primary
        {# , gender_groups #}
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
    FROM {{ ref('stg_idm__user_profiles') }}
    WHERE ( age IS NOT NULL
    OR gender_groups IS NOT NULL
    OR leading_lifestyles IS NOT NULL
    OR metrotech IS NOT NULL
    OR aspirational IS NOT NULL
    OR hearth_home IS NOT NULL
    OR doing_fine IS NOT NULL
    OR fair_go IS NOT NULL
    OR urban_accessibility IS NOT NULL
    OR trades IS NOT NULL
    OR large_scale_homeowners IS NOT NULL
    OR affluent_rural IS NOT NULL)

)

SELECT * FROM account_management__user_profiles
