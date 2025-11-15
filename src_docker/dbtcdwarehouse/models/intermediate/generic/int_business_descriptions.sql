{{
    config(
        materialized = 'incremental',
        tags = ['ml_business_descriptions'],
    )
}}

WITH businessnames AS (

    SELECT stg_naviga__companies.Name_ID
        , stg_naviga__companies.Company_Name
        , stg_naviga__companies.Brand_id
        , stg_naviga__companies.Brand_Name
        , CASE WHEN stg_naviga__companies.Company_Name = stg_naviga__companies.Brand_Name AND stg_naviga__companies.Company_Name IS NOT NULL THEN stg_naviga__companies.Company_Name
            WHEN stg_naviga__companies.Company_Name != stg_naviga__companies.Brand_Name AND stg_naviga__companies.Company_Name IS NOT NULL AND stg_naviga__companies.Brand_Name IS NOT NULL THEN CONCAT(stg_naviga__companies.Company_Name, ' ', stg_naviga__companies.Brand_Name)
          ELSE CONCAT(IF(stg_naviga__companies.Company_Name IS NOT NULL, stg_naviga__companies.Company_Name, '(not set)'), ' ', IF(stg_naviga__companies.Brand_Name IS NOT NULL, stg_naviga__companies.Brand_Name, '(not set)'))
          END AS business_name
     FROM {{ ref('stg_naviga__companies') }} stg_naviga__companies
     LEFT JOIN {{ this }} this ON this.Name_ID = stg_naviga__companies.Name_ID
        AND COALESCE(this.Company_Name, '') = COALESCE(stg_naviga__companies.Company_Name, '')
        AND COALESCE(this.Brand_id, '') = COALESCE(stg_naviga__companies.Brand_id, '')
        AND COALESCE(this.Brand_Name, '') = COALESCE(stg_naviga__companies.Brand_Name, '')
     WHERE is_person = FALSE
     AND this.Name_ID IS NULL
     LIMIT 250

)

SELECT
    Name_ID,
    Company_Name,
    Brand_id,
    Brand_Name,
    classification,
FROM
    {{
        generate_business_description(
        ref('remote_gemini'),
        '(SELECT Name_ID, Company_Name, Brand_id, Brand_Name, business_name FROM businessnames)',
        'business_name',
        'Name_ID',
        'Company_Name',
        'Brand_id',
        'Brand_Name')
    }}
