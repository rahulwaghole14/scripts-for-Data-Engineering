{{
  config(
    tags = ['ml_business_descriptions'],
    )
}}

WITH businessnames AS (

    SELECT * FROM {{ source('adw', 'naviga_companies') }}

)

SELECT * FROM businessnames
