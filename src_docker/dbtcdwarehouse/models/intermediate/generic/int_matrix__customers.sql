{{
  config(
    tags='bq_matrix'
  )
}}

WITH subscribers AS (
    SELECT * FROM {{ ref('stg_matrix__bq_subscribers') }}
)

, persons AS (
    SELECT * FROM {{ ref('stg_matrix__bq_persons') }}
)

, locations AS (
    SELECT * FROM {{ ref('stg_matrix__bq_locations') }}
)

, countries AS (
    SELECT * FROM {{ ref('stg_matrix__bq_countries') }}
)

, contact_numbers AS (

    SELECT
        ObjectPointer
        , MAX(CASE WHEN ContactType = 'HOME' THEN CONCAT(AreaCode, ContactNumber) END) AS HOME
        , MAX(CASE WHEN ContactType = 'WORK' THEN CONCAT(AreaCode, ContactNumber) END) AS WORK
        , MAX(CASE WHEN ContactType = 'MOBILE' THEN CONCAT(AreaCode, ContactNumber) END) AS MOBILE
        , MAX(donotcall) AS DoNotCall
    FROM
        {{ ref('stg_matrix__bq_contact_numbers') }}
    GROUP BY
        ObjectPointer

)

SELECT
DISTINCT
    subscribers.hash_key
    , subscribers.subs_id
    , subscribers.subtype_id
    , persons.title
    , UPPER(persons.FirstName) AS FirstName
    , UPPER(persons.SurName) AS SurName
    , persons.PersContact3
    , contact_numbers.HOME
    , contact_numbers.WORK
    , contact_numbers.MOBILE
    , contact_numbers.DoNotCall
    , locations.AddrLine3
    , locations.AddrLine4
    , locations.AddrLine5
    , locations.AddrLine6
    , countries.country_name
FROM subscribers
LEFT JOIN persons
    ON subscribers.subs_perid = persons.person_pointer
LEFT JOIN locations
    ON persons.ObjectPointer = locations.Level1Pointer
LEFT JOIN countries
    ON locations.Country_Code = countries.Country_Code
LEFT JOIN contact_numbers
    ON persons.ObjectPointer = contact_numbers.ObjectPointer
