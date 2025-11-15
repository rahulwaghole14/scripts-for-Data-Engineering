{{
  config(
        tags=['adw']
    )
}}

WITH adbook AS (

    SELECT * FROM {{ ref('int_ad_revenue__adbook') }}
)

, genera AS (

    SELECT * FROM {{ ref('int_ad_revenue__genera') }}
)

, showcaseturbo AS (

    SELECT * FROM {{ ref('int_ad_revenue__showcaseturbo') }}
)

, showcase_plus AS (

    SELECT * FROM {{ ref('int_ad_revenue__showcase_plus') }}
)

, naviga AS (

    SELECT * FROM {{ ref('int_ad_revenue__naviga') }}
)

, final AS (
    SELECT * FROM adbook
    UNION ALL SELECT * FROM genera
    UNION ALL SELECT * FROM showcaseturbo
    UNION ALL SELECT * FROM showcase_plus
    UNION ALL SELECT * FROM naviga
)

SELECT
    f.*
    , d.FINANCIAL_YEAR
    , d.FINANCIAL_QUARTER
    , d.FINANCIAL_MONTH
FROM final f
LEFT JOIN {{ ref('stg_date_details') }} d ON f.DATE = d.DATE
